(ns job-streamer.control-bus.component.recoverer
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [meta-merge.core :refer [meta-merge]]
            [clojure.core.async :refer [go-loop <! timeout close!]]
            (job-streamer.control-bus.component [datomic :as d]
                                                [agents :as ag]
                                                [jobs :as jobs])))

(defprotocol IRecoverer
  (update-job-status [this]))

(defn- running-jobs [datomic]
  (d/query datomic
           '{:find [[?execution ...]]
             :where [(or [?execution :job-execution/batch-status :batch-status/starting]
                         [?execution :job-execution/batch-status :batch-status/started]
                         [?execution :job-execution/batch-status :batch-status/stopping]
                         [?execution :job-execution/batch-status :batch-status/unknown])
                     [?job :job/executions ?execution]
                     [?app :application/jobs ?job]
                     [?app :application/name]]}))

(defrecord Recoverer []
  component/Lifecycle

  (start [component]
    (let [component (assoc component :interval (atom (:initial-interval component)))
          main-loop (go-loop []
                      (<! (timeout @(:interval component)))
                      (try
                        (update-job-status component)
                        (catch Throwable t
                          (log/error t)))
                      (recur))]
      (assoc component :main-loop main-loop)))

  (stop [component]
    (when-let [main-loop (:main-loop component)]
      (close! main-loop))
    (dissoc component :main-loop))

  IRecoverer
  (update-job-status [{:keys [datomic agents jobs]}]
    (let [execution-ids (running-jobs datomic)]
      (doseq [execution-id execution-ids]
        (let [execution (d/pull datomic
                                '[{:job-execution/agent [:agent/instance-id]
                                   :job-execution/batch-status [:db/ident]}
                                  :job-execution/execution-id] execution-id)
              agt (first (filter #(= (:agent/instance-id %)
                                     (get-in execution [:job-execution/agent :agent/instance-id])) (ag/available-agents agents)))]
          (if agt
            (ag/update-execution agt (:job-execution/execution-id execution)
                                 :on-success (fn [response]
                                               (log/debug "Update execution " response)
                                               (jobs/save-execution jobs execution-id response)))
            (d/transact datomic
                        [{:db/id execution-id
                          :job-execution/batch-status :batch-status/unknown}])))))))

(defn recoverer-component [options]
  (map->Recoverer (meta-merge {:initial-interval 10000}
                              options)))
