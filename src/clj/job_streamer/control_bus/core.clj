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
                                      (broadcast :as broadcast)
                                      (server :as server)
                                      (scheduler :as scheduler)
                                      (dispatcher :as dispatcher)))
  (:use [clojure.core.async :only [chan put! <! go-loop timeout]]
        [liberator.representation :only [ring-response]]
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
                                       [?job :job/steps ?step]
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

(defn find-latest-execution [executions]
  (when executions
    (->> executions
         (sort #(compare (:job-execution/start-time %2)
                         (:job-execution/start-time %1)))
         first)))

(defn find-next-execution [job]
  (when (:job/schedule job)
    (let [next-start (first (scheduler/fire-times (:db/id job)))]
      {:job-execution/start-time next-start})))

;;;
;;; Web API resources
;;;

(defresource jobs-resource
  :available-media-types ["application/edn"]
  :allowed-methods [:get :post]
  :malformed? #(parse-edn % :job)
  :post! (fn [{job :job}]
           (model/transact (job/edn->datoms job nil))
           "OK")
  :handle-ok (fn [{{{query "q"} :query-params} :request}]
               (let []
                 (vec (map (fn [{job-id :job/id
                                 executions :job/executions
                                 schedule :job/schedule :as job}]
                             {:job/id job-id
                              :job/executions executions
                              :job/latest-execution (find-latest-execution executions)
                              :job/next-execution   (find-next-execution job)})
                           (job/find-all query))))))

(defresource job-resource [job-id]
  :available-media-types ["application/edn"]
  :allowed-methods [:get :put :delete]
  :malformed? #(parse-edn % ::data)
  :exists? (when-let [job (job/find-by-id job-id)]
             {:job job})
  :put! (fn [{job ::data job-id :job}]
          (model/transact (job/edn->datoms job job-id)))
  :handle-ok (fn [ctx]
               (let [job (model/pull '[:*
                                       {:job/executions
                                        [:job-execution/start-time
                                         :job-execution/end-time
                                         {:job-execution/batch-status [:db/ident]}
                                         {:job-execution/agent [:agent/name]}]}
                                       {:job/schedule [:schedule/cron-notation]}]
                              (:job ctx))
                     total (count (:job/executions job))
                     success (->> (:job/executions job)
                                  (filter #(= (get-in % [:job-execution/batch-status :db/ident]) 
                                              :batch-status/completed))
                                  count)
                     failure (->> (:job/executions job)
                                  (filter #(= (get-in % [:job-execution/batch-status :db/ident])
                                              :batch-status/failed))
                                  count)
                     average (if (= success 0) 0
                                 (/ (->> (:job/executions job)
                                             (filter #(= (get-in % [:job-execution/batch-status :db/ident])
                                                         :batch-status/completed))
                                             (map #(- (.getTime (:job-execution/end-time %))
                                                      (.getTime (:job-execution/start-time %))))
                                             (reduce +))
                                   success))]
                 (-> job
                     (assoc :job/stats {:total total :success success :failure failure :average average}
                            :job/latest-execution (find-latest-execution (:job/executions job)) 
                            :job/next-execution   (find-next-execution job))
                     (dissoc :job/executions)))))

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
                               :job-execution/job-parameters (pr-str (or (::data ctx) {}))}
                              [:db/add job :job/executions #db/id[db.part/user -1]]])))
  :handle-ok (fn [ctx]
               (job/find-executions job-id)))

(defresource execution-resource [job-id id]
  :available-media-types ["application/edn"]
  :allowed-methods [:get]
  :handle-ok (fn [ctx]
               (job/find-execution job-id id)))

(defresource schedule-resource [job-id]
  :available-media-types ["application/edn"]
  :allowed-methods [:post :delete]
  :malformed? #(parse-edn % ::data)
  :exists? (fn [ctx]
             (:job/schedule (model/pull '[:job/schedule] job-id)))
  :post! (fn [ctx]
           (scheduler/schedule job-id (get-in ctx [::data :schedule/cron-notation])))
  :delete! (fn [ctx]
             (scheduler/unschedule job-id))
  :handle-ok (fn [ctx])
  :handle-exception (fn [{ex :exception}]
                      (ring-response {:status 500
                                      :body (pr-str {:message (.getMessage ex)})})))

(defresource application-resource [app-id]
  :available-media-types ["application/edn"]
  :allowed-methods [:post]
  :malformed? #(parse-edn % ::data)
  :post! (fn [ctx]
           )
  :handle-ok (fn [ctx]))
(defn wrap-same-origin-policy [handler]
  (fn [req]
    (if (= (:request-method req) :options)
      ;;Pre-flight request
      {:status 200
       :headers {"Access-Control-Allow-Methods" "POST,GET,PUT,DELETE,OPTIONS"
                 "Access-Control-Allow-Origin" "*"
                 "Access-Control-Allow-Headers" "Content-Type"}}
      (when-let [resp (handler req)]
        (header resp "Access-Control-Allow-Origin" "*")))))

(defroutes app-routes
  (ANY "/jobs" [] jobs-resource)
  (ANY ["/job/:job-id/executions" :job-id #".*"] [job-id] (executions-resource job-id))
  (ANY ["/job/:job-name/schedule" :job-name #".*"] [job-name]
    (schedule-resource (job/find-by-id job-name)))
  (ANY ["/job/:job-id/execution/:id" :job-id #".*" :id #"\d+"]
      [job-id id]
    (execution-resource job-id (Long/parseLong id)))
  (ANY "/job/:id"  [id] (job-resource id))
  (ANY "/agents" [] ag/agents-resource)
  (ANY "/deploy" [] )
  (GET "/logs" [] (pr-str (model/query '{:find [[(pull ?log [*]) ...]] :where [[?log :execution-log/level]]}))))

(defn -main [& {:keys [port] :or {port 45102}}]
  (init)
  (broadcast/start port)
  (scheduler/start "localhost" port)
  (dispatcher/start)
  (go-loop []
    (let [jobs (job/find-undispatched)]
      (doseq [[execution-request job parameter] jobs]
        (dispatcher/submit {:request-id execution-request
                            :job job
                            :parameters parameter}))
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
