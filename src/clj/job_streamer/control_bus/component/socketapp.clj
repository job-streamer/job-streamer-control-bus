(ns job-streamer.control-bus.component.socketapp
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [put!]]
            [clojure.edn :as edn]
            (job-streamer.control-bus.component [datomic :as datomic]
                                                [agents :as ag]
                                                [jobs :as job])))

(defmulti handle-command (fn [socketapp msg ch] (:command msg)))

(defmethod handle-command :ready [{:keys [agents]} command ch]
  (log/info "ready " command)
  (ag/ready agents ch command))

(defmethod handle-command :progress [{:keys [agents jobs]} {:keys [id execution-id]} ch]
  (log/debug "Progress execution" execution-id)
  (ag/update-execution
   (ag/find-agent-by-channel agents ch)
   execution-id
   :on-success (fn [response]
                 (job/save-execution jobs id response))))

(defmethod handle-command :start-step [{:keys [agents datomic]}
                                       {:keys [id execution-id step-execution-id
                                               step-name instance-id]} ch]
  (log/debug "start-step" step-name execution-id step-execution-id)
  (if-let [step (datomic/query datomic
                               '{:find [?step .]
                                 :in [$ ?id ?step-name]
                                 :where [[?job :job/executions ?id]
                                         [?job :job/steps ?step]
                                         [?step :step/name ?step-name]]}
                               id step-name)]
    (datomic/transact datomic
                      [{:db/id #db/id[db.part/user -1]
                        :step-execution/step step
                        :step-execution/step-execution-id step-execution-id
                        :step-execution/batch-status :batch-status/starting}
                       [:db/add id
                        :job-execution/step-executions  #db/id[db.part/user -1]]])))

(defmethod handle-command :progress-step [{:keys [agents jobs datomic]}
                                          {:keys [id execution-id step-execution-id
                                                  instance-id]} ch]
  (ag/update-step-execution
   (ag/find-agent-by-channel agents ch)
   execution-id
   step-execution-id
   
   :on-success
   (fn [step-execution]
     (if-let [id (job/find-step-execution jobs instance-id step-execution-id)]
       (do
         (log/debug "progress-step !" id)
         (datomic/transact datomic
                           [{:db/id id
                             :step-execution/batch-status (step-execution :batch-status)
                             :step-execution/exit-status  (step-execution :exit-status)
                             :step-execution/start-time (step-execution :start-time)
                             :step-execution/end-time (step-execution :end-time)}]))
       (log/warn "Not found a step execution.  instance-id=" instance-id
                 ", step-execution-id=" step-execution-id)))))

(defmethod handle-command :bye [{:keys [agents]} _ ch]
  (ag/bye agents ch))

(defrecord SocketApp [datomic jobs agents]
  component/Lifecycle

  (start [component]
    (assoc component
           :path "/join"
           :on-message (fn [ch message]
                         (when (not-empty message)
                           (handle-command component (edn/read-string message) ch)))
           :on-close (fn [ch close-reason]
                       (log/info "disconnect" ch "for" close-reason)
                       (handle-command component {:command :bye} ch))))

  (stop [component]
    (dissoc component :path :on-message :on-close)))

(defn socketapp-component [options]
  (map->SocketApp options))
