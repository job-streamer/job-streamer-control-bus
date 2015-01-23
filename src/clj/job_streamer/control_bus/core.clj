(ns job-streamer.control-bus.core
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [liberator.core :refer [defresource]]
            [compojure.route :as route]
            [compojure.core :refer [defroutes GET ANY]]
            [ring.middleware.defaults :refer [wrap-defaults  api-defaults]]
            [ring.middleware.reload :refer [wrap-reload]]
            [datomic.api :as d]
            (job-streamer.control-bus (model :as model)
                                      (job :as job)
                                      (multicast :as multicast)
                                      (server :as server)
                                      (dispatcher :as dispatcher)))
  (:use [clojure.core.async :only [chan put! <! go-loop timeout]]
        [job-streamer.control-bus.agent :only [find-agent execute-job] :as ag]
        [ring.util.response :only [header]]))

(defonce config (atom {}))

(defn init []
  (model/create-schema))

(defn- body-as-string [ctx]
  (if-let [body (get-in ctx [:request :body])]
    (condp instance? body
      java.lang.String body
      (slurp (io/reader body)))))

(defn- parse-edn [context key]
  (when (#{:put :post} (get-in context [:request :request-method]))
    (try
      (if-let [body (body-as-string context)]
        (let [data (edn/read-string body)]
          [false {key data}])
        false)
      (catch Exception e
        (log/error "fail to parse edn." e)
        (:message (format "IOException: %s" (.getMessage e)))))))

(defmulti handle-command (fn [msg ch] (:command msg)))

(defmethod handle-command :ready [command ch]
  (log/info "ready " command)
  (ag/ready ch command))

(defmethod handle-command :progress [{:keys [id execution-id]} ch]
  (log/debug "Progress execution" execution-id)
  (ag/update-execution
   (ag/find-agent-by-channel ch)
   execution-id
   :on-success (fn [execution]
                 (log/debug "progress update: " id execution)
                 (model/transact [{:db/id id
                                   :job-execution/batch-status (execution :batch-status)
                                   :job-execution/start-time (execution :start-time)
                                   :job-execution/end-time (execution :end-time)}]))))

(defmethod handle-command :start-step [{:keys [id execution-id step-execution-id step-name instance-id]} ch]
  (log/debug "start-step" step-name execution-id step-execution-id)
  (if-let [step (model/query '{:find [?step .]
                               :in [$ ?id ?step-name]
                               :where [[?job :job/executions ?id]
                                       [?job :job/step ?step]
                                       [?step :step/id ?step-name]]}
                             id step-name)]
    (model/transact [{:db/id #db/id[db.part/user -1]
                      :step-execution/step step
                      :step-execution/step-execution-id step-execution-id
                      :step-execution/batch-status :batch-status/starting}
                     [:db/add id
                      :job-execution/step-executions  #db/id[db.part/user -1]]])))

(defmethod handle-command :progress-step [{:keys [id execution-id step-execution-id instance-id]} ch]
  (ag/update-step-execution
   (ag/find-agent-by-channel ch)
   execution-id step-execution-id
   :on-success (fn [step-execution]
                 (if-let [id (job/find-step-execution instance-id step-execution-id)]
                   (do
                     (log/debug "progress-step !" id)
                     (model/transact [{:db/id id
                                       :step-execution/batch-status (step-execution :batch-status)
                                       :step-execution/start-time (step-execution :start-time)
                                       :step-execution/end-time (step-execution :end-time)}]))
                   (log/warn "Not found a step execution.  instance-id=" instance-id
                             ", step-execution-id=" step-execution-id)))))

(defmethod handle-command :bye [_ ch]
  (ag/bye ch))

(defresource job-resource [job-id]
  :available-media-types ["application/edn"]
  :allowed-methods [:get :put :delete]
  :malformed? #(parse-edn % ::data)
  :exists? (when-let [job (job/find-by-id job-id)]
             {:job job})
  :handle-ok (fn [ctx]
               (model/pull '[:*] (:job ctx))))

(defresource execution-resource [job-id id]
  :available-media-types ["application/edn"]
  :allowed-methods [:get]
  :handle-ok (fn [ctx]
               (job/find-execution job-id id)))

(defresource executions-resource [job-id]
  :available-media-types ["application/edn"]
  :allowed-methods [:get :post]
  :malformed? #(parse-edn % ::data)
  :exists? (when-let [job (job/find-by-id job-id)]
             {:job job})
  :if-match-star-exists-for-missing? (fn [{job :job}]
                                       (nil? job))
  :post! (fn [ctx]
           (when-let [job (job/find-by-id job-id)]
             (model/transact [{:db/id #db/id[db.part/user -1]
                               :job-execution/batch-status :batch-status/registered
                               :job-execution/job-parameters ""}
                              [:db/add job :job/executions #db/id[db.part/user -1]]])))
  :handle-ok (fn [ctx]
               (job/find-executions job-id)))

(defresource jobs-resource
  :available-media-types ["application/edn"]
  :allowed-methods [:get :post]
  :malformed? #(parse-edn % ::data)
  :post! (fn [{{job :job} ::data}]
           (println job)
           (println (job/edn->datoms job))
           (model/transact (job/edn->datoms job))
           "OK")
  :handle-ok (fn [{{{query "q"} :query-params} :request}]
               (let []
                 (vec (map (fn [{job-id :job/id executions :job/executions}]
                             (merge {:job/id job-id
                                     :job/executions executions}
                                    (when executions
                                      {:job/last-execution
                                       (->> executions
                                            (sort #(compare (:job-execution/start-time %2)
                                                              (:job-execution/start-time %1)))
                                            first)})))
                           (job/find-all query))))))

(defn wrap-same-origin-policy [handler]
  (fn [req]
    (when-let [resp (handler req)]
      (header resp "Access-Control-Allow-Origin" "*"))))

(defroutes app-routes
  (ANY "/jobs" [] jobs-resource)
  (ANY ["/job/:job-id/executions" :job-id #".*"] [job-id] (executions-resource job-id))
  (ANY ["/job/:job-id/execution/:id" :job-id #".*" :id #"\d+"]
      [job-id id]
    (execution-resource job-id (Long/parseLong id)))
  (ANY "/job/:id"  [id] (job-resource id))
  (ANY "/agents" [] ag/agents-resource)
  (GET "/logs" [] (pr-str (model/query '{:find [[(pull ?log [*]) ...]] :where [[?log :execution-log/level]]}))))

(defn -main [& {:keys [port] :or {port 45102}}]
  (init)
  (multicast/start port)
  (dispatcher/start)
  (go-loop []
    (let [jobs (job/find-undispatched)]
      (doseq [[execution-request job parameter] jobs]
        (dispatcher/submit {:request-id execution-request
                             :job job
                             :parameter parameter}))
      (<! (timeout 2000))
      (recur)))

  (server/run-server
   (-> app-routes
       (wrap-defaults api-defaults)
       (wrap-same-origin-policy)
       wrap-reload)
   :port port
   :websockets [{:path "/join"
                 :on-message (fn [ch message]
                               (handle-command (merge (edn/read-string message)
                                                      {:host "localhost"}) ch))
                 :on-close (fn [ch close-reason]
                             (log/info "disconnect" ch "for" close-reason)
                             (handle-command {:command :bye} ch))}]))
