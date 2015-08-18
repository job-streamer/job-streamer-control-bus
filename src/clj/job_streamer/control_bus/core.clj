(ns job-streamer.control-bus.core
  (:require [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [compojure.route :as route]
            [compojure.core :refer [defroutes GET ANY]]
            [ring.middleware.defaults :refer [wrap-defaults  api-defaults]]
            [ring.middleware.reload :refer [wrap-reload]]
            (job-streamer.control-bus (model :as model)
                                      (job :as job)
                                      (apps :as apps)
                                      (agent :as ag)
                                      (broadcast :as broadcast)
                                      (server :as server)
                                      (scheduler :as scheduler)
                                      (dispatcher :as dispatcher)
                                      (recovery :as recovery)))
  (:use [clojure.core.async :only [chan put! <! go go-loop timeout]]
        [environ.core :only [env]]
        [liberator.representation :only [ring-response]]
        [job-streamer.control-bus.api]
        [ring.util.response :only [header]])
  (:gen-class))

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
   :on-success (fn [response]
                 (job/save-execution id response))))

(defmethod handle-command :start-step [{:keys [id execution-id step-execution-id step-name instance-id]} ch]
  (log/debug "start-step" step-name execution-id step-execution-id)
  (if-let [step (model/query '{:find [?step .]
                               :in [$ ?id ?step-name]
                               :where [[?job :job/executions ?id]
                                       [?job :job/steps ?step]
                                       [?step :step/name ?step-name]]}
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
                                       :step-execution/exit-status  (step-execution :exit-status)
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
  (ANY "/:app-name/jobs" [app-name] (jobs-resource app-name) )
  (ANY ["/:app-name/job/:job-name/settings/:cmd" :app-name #".*" :job-name #".*" :cmd #"[\w\-]+"]
      [app-name job-name cmd]
    (job-settings-resource app-name job-name (keyword cmd)))
  (ANY ["/:app-name/job/:job-name/settings" :app-name #".*" :job-name #".*"]
      [app-name job-name]
    (job-settings-resource app-name job-name))
  (ANY ["/:app-name/job/:job-name/executions" :app-name #".*" :job-name #".*"]
      [app-name job-name]
    (executions-resource app-name job-name))
  (ANY ["/:app-name/job/:job-name/schedule" :app-name #".*" :job-name #".*"]
      [app-name job-name]
    (let [[_ job-id] (job/find-by-name app-name job-name)]
      (schedule-resource job-id)))
  (ANY ["/:app-name/job/:job-name/schedule/:cmd" :app-name #".*" :job-name #".*" :cmd #"\w+"]
      [app-name job-name cmd]
    (let [[_ job-id] (job/find-by-name app-name job-name)]
      (schedule-resource job-id (keyword cmd))))
  (ANY ["/:app-name/job/:job-name/execution/:id" :app-name #".*" :job-name #".*" :id #"\d+"]
      [app-name job-name id]
    (execution-resource (Long/parseLong id)))
  (ANY ["/:app-name/job/:job-name/execution/:id/:cmd" :app-name #".*" :job-name #".*" :id #"\d+" :cmd #"\w+"]
      [app-name job-name id cmd]
    (execution-resource (Long/parseLong id) (keyword cmd)))
  (ANY ["/:app-name/job/:job-name" :app-name #".*" :job-name #".*"]
      [app-name job-name] (job-resource app-name job-name))
  (ANY "/:app-name/stats" [app-name]
    (stats-resource app-name))
  (ANY ["/calendar/:cal-name" :cal-name #".*"] [cal-name]
    (calendar-resource cal-name))
  (ANY "/calendars" [] calendars-resource)
  (ANY "/agents" [] ag/agents-resource)
  (ANY ["/agent/:instance-id/:cmd" :instance-id #"[A-Za-z0-9\-]+" :cmd #"\w+"]
      [instance-id cmd]
    (ag/agent-resource instance-id (keyword cmd)))

  (ANY "/agent/:instance-id" [instance-id]
    (ag/agent-resource instance-id))
  (ANY "/agent/:instance-id/monitor/:type/:cycle" [instance-id type cycle]
    (ag/agent-monitor-resource instance-id type cycle))
  (ANY "/apps" [] applications-resource)
  (ANY ["/:app-name/batch-components" :app-name #".*"]
      [app-name]
    (batch-components-resource app-name))
  ;; For debug
  (GET "/logs" [] (pr-str (model/query '{:find [[(pull ?log [*]) ...]] :where [[?log :execution-log/level]]})))
  (route/not-found {:content-type "application/edn"
                    :body "{:message \"not found\"}"}))

(defn -main [& args]
  (init)
  (let [port (Integer/parseInt (or (:control_bus-port env) "45102"))]
    (broadcast/start port)
    (go (scheduler/start "localhost" port))
    (dispatcher/start)
    (go
      (let [applications (model/query '{:find [[(pull ?app [*])]]
                                        :where [[?app :application/name]]})]
        (doseq [app applications]
          (apps/register (assoc app :name "default")))))
    (go-loop []
      (let [jobs (job/find-undispatched)]
        (doseq [[execution-request job parameter] jobs]
          (dispatcher/submit {:request-id execution-request
                              :class-loader-id (:application/class-loader-id (apps/find-by-name "default"))
                              :job job
                              :parameters parameter}))
        (<! (timeout 2000))
        (recur)))
    (ag/start-monitor)

    (go-loop []
      (<! (timeout 10000))
      (recovery/update-job-status)
      (recur))

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
                               (handle-command {:command :bye} ch))}])))
