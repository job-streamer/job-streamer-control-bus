(ns job-streamer.control-bus.component.dispatcher
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [clojure.core.async :refer [chan put! <! go-loop timeout close!]]
            (job-streamer.control-bus.component [agents :as ag]
                                                [apps :as apps]
                                                [datomic :as d]
                                                [jobs :as jobs])))

(defn- dispatch [{:keys [dispatcher-ch datomic agents]} agt execution-request]
  (ag/execute-job
   agents
   agt execution-request
   
   :on-error
   (fn [status e]
     (log/error "failure submit job [" (get-in execution-request [:job :job/name])
                "] at host [" (:host agt) "]" e)
     (if (not= status 503)
       (d/transact datomic
                   [{:db/id (:request-id execution-request)
                     :job-execution/agent [:agent/instance-id (:agent/instance-id agt)]
                     :job-execution/batch-status :batch-status/abandoned}])
       (put! dispatcher-ch execution-request)))
   
   :on-success
   (fn [{:keys [execution-id batch-status start-time] :as res}]
     (if execution-id
       (d/transact datomic
                   [(merge {:db/id (:request-id execution-request)
                            :job-execution/execution-id execution-id
                            :job-execution/agent [:agent/instance-id (:agent/instance-id agt)]
                            :job-execution/batch-status batch-status}
                           (when start-time
                             {:job-execution/start-time start-time}))])
       (d/transact datomic
                   [{:db/id (:request-id execution-request)
                     :job-execution/agent [:agent/instance-id (:agent/instance-id agt)]
                     :job-execution/batch-status :batch-status/abandoned}])))))

(defn submit [{:keys [dispatcher-ch datomic]} execution-request]
  (try
    (put! dispatcher-ch execution-request)
    (d/transact datomic
                [{:db/id (:request-id execution-request)
                  :job-execution/batch-status :batch-status/queued}])
    (catch Exception ex
      (log/error "dispatch failure" ex))))

(defn submitter [{:keys [jobs apps] :as dispatcher}]
  (go-loop []
    (let [undispatched (jobs/find-undispatched jobs)]
      (doseq [[execution-request job parameter] undispatched]
        (submit dispatcher
                {:request-id execution-request
                 :class-loader-id (:application/class-loader-id
                                   (apps/find-by-name apps "default"))
                 :job job
                 :parameters parameter}))
      (<! (timeout 2000))
      (recur))))

(defrecord Dispatcher [datomic]
  component/Lifecycle
  (start [component]
    (let [dispatcher-ch (chan)
          main-loop (go-loop []
                      (let [execution-request (<! dispatcher-ch)]
                        (log/info "Dispatch request for " execution-request)
                        (loop [agt (ag/find-agent), log-interval 0]
                          (if agt
                            (dispatch (assoc component :dispatcher-ch dispatcher-ch)
                                      agt execution-request)
                            (do
                              (if (= (mod log-interval 10) 0)
                                (log/info "No available agents for " execution-request))
                              (<! (timeout 3000))
                              (recur (ag/find-agent) (inc log-interval)))))
                        (recur)))
          submit-loop (submitter (assoc component
                                        :dispatcher-ch dispatcher-ch))]
      (assoc component
             :dispatcher-ch dispatcher-ch
             :main-loop main-loop
             :submit-loop submit-loop)))

  (stop [component]
    (if-let [main-loop (:main-loop component)]
      (close! main-loop))
    (if-let [submit-loop (:submit-loop component)]
      (close! submit-loop))
    (dissoc component :dispatch-ch :main-loop :submit-loop)))

(defn dispatcher-component [options]
  (map->Dispatcher options))
