(ns job-streamer.control-bus.job
  (:require [datomic.api :as d]
            [job-streamer.control-bus.model :as model]))


(defn find-undispatched []
  (model/query
   '{:find [?job-execution ?job-obj ?parameter]
    :where [[?job-execution :job-execution/batch-status :batch-status/registered]
            [?job-execution :job-execution/job-parameters ?parameter]
            [?job :job/executions ?job-execution]
            [?job :job/edn-notation ?edn-notation]
            [(clojure.edn/read-string ?edn-notation) ?job-obj]]}))

(defn find-by-id [job-id]
  (model/query 
   '[:find ?e .
     :in $ ?job-id 
     :where [?e :job/id ?job-id]]
   job-id))

(defn find-all [q]
  (let [base-query '{:find [[(pull ?job [:* {:job/executions [:db/id
                                                              :job-execution/start-time
                                                              :job-execution/end-time
                                                              {:job-execution/batch-status [:db/ident]}]}]) ...]]
                     :in [$ ?query]
                     :where [[?job :job/id ?job-id]]}]
    (model/query
     (if (not-empty q)
       (update-in base-query [:where] conj '[(fulltext $ :job/id ?query) [[?job ?job-id]]])
       base-query)
     (or q ""))))

(defn find-executions [job-id]
  (vec (model/query
        '{:find [(pull ?job-execution [*])]
          :in [$ ?job-id]
          :where [[?job :job/id ?job-id]
                  [?job :job/executions ?job-execution]]}
        job-id)))

(defn find-execution [job-id job-execution]
  (let [je (model/pull '[:*
                        {:job-execution/agent [:agent/instance-id :agent/name]}
                        {:job-execution/step-executions
                         [:*
                          {:step-execution/batch-status [:db/ident]}
                          {:step-execution/step [:step/id]}]}] job-execution)] 
    (update-in je [:job-execution/step-executions]
               #(map (fn [step-execution]
                       (assoc step-execution :step-execution/logs
                              (model/query '{:find [[(pull ?log [:* {:execution-log/level [:db/ident]}]) ...]]
                                             :in [$ ?step-execution-id ?instance-id]
                                             :where [[?log :execution-log/step-execution-id ?step-execution-id]
                                                     [?log :execution-log/agent ?agent]
                                                     [?agent :agent/instance-id ?instance-id]]}
                                           (:step-execution/step-execution-id step-execution)
                                           (get-in je [:job-execution/agent :agent/instance-id])))) %))))

(defn find-step-execution [instance-id step-execution-id]
  (model/query
   '{:find [?step-execution .]
     :in [$ ?instance-id ?step-execution-id]
     :where [[?job-execution :job-execution/agent ?agent]
             [?agent :agent/instance-id ?instance-id]
             [?job-execution :job-execution/step-executions ?step-execution]
             [?step-execution :step-execution/step-execution-id ?step-execution-id]]}
   instance-id step-execution-id))

(defn edn->datoms [job]
  (let [datoms (atom [])
        step-refs (doall
                   (for [step (:steps job)]
                       (let [id (d/tempid :db.part/user)]
                         (swap! datoms conj
                                (merge
                                 {:db/id id
                                  :step/id (:id step)
                                  :step/start-limit (get job :start-limit 0)
                                  :step/allow-start-if-complete (get job :allow-start-if-complete false)}
                                 (when-let [batchlet (:batchlet step)]
                                   (let [batchlet-id (d/tempid :db.part/user)]
                                     (swap! datoms conj {:db/id batchlet-id :batchlet/ref (:ref batchlet)})
                                     {:step/batchlet batchlet-id}))))
                         id)))]
    
    (conj @datoms
          {:db/id (d/tempid :db.part/user)
           :job/id (:id job)
           :job/restartable (:restartable job)
           :job/edn-notation (pr-str job)
           :job/step step-refs})))
