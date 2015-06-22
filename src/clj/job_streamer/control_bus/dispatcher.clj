(ns job-streamer.control-bus.dispatcher
  (:use [clojure.core.async :only [chan put! <! go-loop timeout]])
  (:require [clojure.tools.logging :as log]
            (job-streamer.control-bus (agent :as ag)
                                      (model :as model))))

(defonce dispatcher-ch (chan))

(defn- dispatch [agt execution-request]
  (ag/execute-job agt execution-request
               :on-error (fn [e]
                           (log/error "failure submit job [" (get-in execution-request [:job :job/name])
                                      "] at host [" (:host agt) "]" e)
                           (put! dispatcher-ch execution-request))
               :on-success (fn [{:keys [execution-id batch-status start-time] :as res}]
                             (println res)
                             (if execution-id
                               (model/transact [(merge {:db/id (:request-id execution-request)
                                                        :job-execution/execution-id execution-id
                                                        :job-execution/agent [:agent/instance-id (:agent/instance-id agt)]
                                                        :job-execution/batch-status batch-status}
                                                       (when start-time
                                                         {:job-execution/start-time start-time}))])
                               (model/transact [{:db/id (:request-id execution-request)
                                                 :job-execution/agent [:agent/instance-id (:agent/instance-id agt)]
                                                 :job-execution/batch-status :batch-status/abandoned}])))))

(defn submit [execution-request]
  (try
    (put! dispatcher-ch execution-request)
    (model/transact [{:db/id (:request-id execution-request)
                      :job-execution/batch-status :batch-status/queued}])
    (catch Exception ex
      (log/error "dispatch failure" ex))))

(defn start []
  (go-loop []
    (let [execution-request (<! dispatcher-ch)]
      (log/info "Dispatch request for " execution-request)
      (loop [agt (ag/find-agent), log-interval 0]
        (if agt
          (dispatch agt execution-request)
          (do
            (if (= (mod log-interval 10) 0)
              (log/info "No available agents for " execution-request))
            (<! (timeout 3000))
            (recur (ag/find-agent) (inc log-interval)))))
      (recur))))
