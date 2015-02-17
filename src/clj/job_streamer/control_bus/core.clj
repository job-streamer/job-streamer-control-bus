(ns job-streamer.control-bus.core
  (:require [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [compojure.route :as route]
            [compojure.core :refer [defroutes GET ANY]]
            [ring.middleware.defaults :refer [wrap-defaults  api-defaults]]
            [ring.middleware.reload :refer [wrap-reload]]
            (job-streamer.control-bus (model :as model)
                                      (job :as job)
                                      (agent :as ag)
                                      (broadcast :as broadcast)
                                      (server :as server)
                                      (scheduler :as scheduler)
                                      (dispatcher :as dispatcher)))
  (:use [clojure.core.async :only [chan put! <! go-loop timeout]]
        [liberator.representation :only [ring-response]]
        [job-streamer.control-bus.api :only [jobs-resource job-resource
                                             executions-resource execution-resource
                                             schedule-resource]]
        [ring.util.response :only [header]]))

(defonce config (atom {}))

(defn init []
  (model/create-schema))


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
  ;; For debug
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
                               (handle-command (edn/read-string message) ch))
                 :on-close (fn [ch close-reason]
                             (log/info "disconnect" ch "for" close-reason)
                             (handle-command {:command :bye} ch))}]))
