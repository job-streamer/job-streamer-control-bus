(ns job-streamer.control-bus.recovery
  (:require (job-streamer.control-bus (model :as model)
                                      (agent :as ag)
                                      (job :as job))))

(defn update-job-status []
  (let [execution-ids (model/query '{:find [[?execution ...]]
                                 :where [(or [?execution :job-execution/batch-status :batch-status/starting]
                                             [?execution :job-execution/batch-status :batch-status/started]
                                             [?execution :job-execution/batch-status :batch-status/stopping]
                                             [?execution :job-execution/batch-status :batch-status/unknown])
                                         [?job :job/executions ?execution]
                                         [?app :application/jobs ?job]
                                         [?app :application/name]]})]
    (doseq [execution-id execution-ids]
      (let [execution (model/pull '[{:job-execution/agent [:agent/instance-id]
                                     :job-execution/batch-status [:db/ident]}
                                    :job-execution/execution-id] execution-id)
            agt (first (filter #(= (:agent/instance-id %)
                                   (get-in execution [:job-execution/agent :agent/instance-id])) (ag/available-agents)))]
        (if agt
          (ag/update-execution agt (:job-execution/execution-id execution)
                               :on-success (fn [response]
                                             (job/save-execution execution-id response)))
          (model/transact [{:db/id execution-id :job-execution/batch-status :batch-status/unknown}]))))))
