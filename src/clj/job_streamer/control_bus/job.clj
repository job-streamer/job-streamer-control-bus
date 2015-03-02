(ns job-streamer.control-bus.job
  (:require [datomic.api :as d]
            [job-streamer.control-bus.model :as model]))


(defn find-undispatched []
  (model/query
   '{:find [?job-execution ?job-obj ?param-map]
     :where [[?job-execution :job-execution/batch-status :batch-status/registered]
             [?job-execution :job-execution/job-parameters ?parameter]
             [?job :job/executions ?job-execution]
             [?job :job/edn-notation ?edn-notation]
             [(clojure.edn/read-string ?edn-notation) ?job-obj]
             [(clojure.edn/read-string ?parameter) ?param-map]]}))

(defn find-by-name [app-name job-name]
  (model/query 
   '{:find [[?app ?job]]
     :in [$ ?app-name ?job-name]
     :where [[?app :application/name ?app-name]
             [?app :application/jobs ?job]
             [?job :job/name ?job-name]]}
   app-name job-name))


(defn find-all [app-name q]
  (let [base-query '{:find [[(pull ?job
                                   [:*
                                    {:job/executions
                                     [:db/id
                                      :job-execution/start-time
                                      :job-execution/end-time
                                      {:job-execution/batch-status [:db/ident]}]}
                                    {:job/schedule
                                     [:db/id :schedule/cron-notation]}]) ...]]
                     :in [$ ?app-name ?query]
                     :where [[?app :application/name ?app-name]
                             [?app :application/jobs ?job]]}]
    (model/query
     (if (not-empty q)
       (update-in base-query [:where] conj '[(fulltext $ :job/name ?query) [[?job ?job-name]]])
       base-query)
     app-name (or q ""))))

(defn find-executions [app-name job-name]
  (->> (model/query
        '{:find [[(pull ?job-execution
                        [:db/id
                         :job-execution/start-time
                         :job-execution/end-time
                         :job-execution/job-parameters
                         {:job-execution/batch-status [:db/ident]
                          :job-execution/agent
                          [:agent/instance-id :agent/name]}]) ...]]
          :in [$ ?app-name ?job-name]
          :where [[?app :application/name ?app-name]
                  [?app :application/jobs ?job]
                  [?job :job/name ?job-name]
                  [?job :job/executions ?job-execution]
                  [?job-execution :job-execution/start-time]]}
        app-name job-name)
       (sort #(compare (:job-execution/start-time %2)
                       (:job-execution/start-time %1)))
       vec))

(defn find-execution [job-execution]
  (let [je (model/pull '[:*
                         {:job-execution/agent [:agent/instance-id :agent/name]}
                         {:job-execution/step-executions
                          [:*
                           {:step-execution/batch-status [:db/ident]}
                           {:step-execution/step [:step/id]}]}] job-execution)] 
    (update-in je [:job-execution/step-executions]
               #(map (fn [step-execution]
                       (assoc step-execution :step-execution/logs
                              (->> (model/query '{:find [[(pull ?log [:* {:execution-log/level [:db/ident]}]) ...]]
                                                  :in [$ ?step-execution-id ?instance-id]
                                                  :where [[?log :execution-log/step-execution-id ?step-execution-id]
                                                          [?log :execution-log/agent ?agent]
                                                          [?agent :agent/instance-id ?instance-id]]}
                                                (:step-execution/step-execution-id step-execution)
                                                (get-in je [:job-execution/agent :agent/instance-id] ""))
                                   (sort (fn [l1 l2]
                                           (compare (:execution-log/date l1)
                                                    (:execution-log/date l2))))))) %))))

(defn find-step-execution [instance-id step-execution-id]
  (model/query
   '{:find [?step-execution .]
     :in [$ ?instance-id ?step-execution-id]
     :where [[?job-execution :job-execution/agent ?agent]
             [?agent :agent/instance-id ?instance-id]
             [?job-execution :job-execution/step-executions ?step-execution]
             [?step-execution :step-execution/step-execution-id ?step-execution-id]]}
   instance-id step-execution-id))

(defn edn->datoms [job job-id]
  (let [datoms (atom [])
        step-refs (doall
                   (for [step (:job/steps job)]
                       (let [id (d/tempid :db.part/user)]
                         (swap! datoms conj
                                (merge
                                 {:db/id id
                                  :step/name (:step/name step)
                                  :step/start-limit (get job :step/start-limit 0)
                                  :step/allow-start-if-complete? (get job :step/allow-start-if-complete? false)}
                                 (when-let [batchlet (:step/batchlet step)]
                                   (let [batchlet-id (d/tempid :db.part/user)]
                                     (swap! datoms conj {:db/id batchlet-id :batchlet/ref (:batchlet/ref batchlet)})
                                     {:step/batchlet batchlet-id}))))
                         id)))]
    
    (into [{:db/id (or job-id (d/tempid :db.part/user)) 
            :job/name (:job/name job)
            :job/restartable? (or (:job/restartable? job) true) 
            :job/edn-notation (pr-str job)
            :job/steps step-refs}] @datoms)))
