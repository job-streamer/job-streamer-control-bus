(ns job-streamer.control-bus.job
  (:require [datomic.api :as d]
            [clojure.tools.logging :as log]
            (job-streamer.control-bus (model :as model)
                                      (notification :as notification))))

(defn find-undispatched []
  (model/query
   '{:find [?job-execution ?job-obj ?param-map]
     :where [[?job-execution :job-execution/batch-status :batch-status/undispatched]
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


(defn find-all [app-name query & [offset limit]]
  (let [base-query '{:find [?job]
                     :in [$ ?app-name ?query]
                     :where [[?app :application/name ?app-name]
                             [?app :application/jobs ?job]]}
        jobs (model/query (if (not-empty query)
                            (update-in base-query [:where] conj '[(fulltext $ :job/name ?query) [[?job ?job-name]]])
                            base-query)
                          app-name (or query ""))]
    {:results (->> jobs
                   (drop (dec offset))
                   (take limit)
                   (map #(model/pull '[:*
                                       {:job/executions
                                        [:db/id
                                         :job-execution/create-time
                                         :job-execution/start-time
                                         :job-execution/end-time
                                         {:job-execution/batch-status [:db/ident]}]}
                                       {:job/schedule
                                        [:db/id :schedule/cron-notation :schedule/active?]}] (first %)))
                   vec)
     :hits   (count jobs)
     :offset offset
     :limit limit}))

(defn find-executions [app-name job-name & [offset limit]]
  (let [executions (model/query '{:find [?job-execution ?create-time]
                                  :in [$ ?app-name ?job-name]
                                  :where [[?job-execution :job-execution/create-time ?create-time]
                                          [?app :application/name ?app-name]
                                          [?app :application/jobs ?job]
                                          [?job :job/name ?job-name]
                                          [?job :job/executions ?job-execution]]} app-name job-name)]
    {:results (->> executions
                   (sort-by second #(compare %2 %1))
                   (drop (dec offset))
                   (take limit)
                   (map #(model/pull '[:db/id
                                       :job-execution/execution-id
                                       :job-execution/create-time
                                       :job-execution/start-time
                                       :job-execution/end-time
                                       :job-execution/job-parameters
                                       {:job-execution/batch-status [:db/ident]
                                        :job-execution/agent
                                        [:agent/instance-id :agent/name]}] (first %)))
                   
                   vec)
     :hits    (count executions)
     :offset  offset
     :limit   limit}))

(defn find-execution [job-execution]
  (let [je (model/pull '[:*
                         {:job-execution/agent [:agent/instance-id :agent/name]}
                         {:job-execution/step-executions
                          [:*
                           {:step-execution/batch-status [:db/ident]}
                           {:step-execution/step [:step/name]}]}] job-execution)]
    (update-in je [:job-execution/step-executions]
               #(map (fn [step-execution]
                       (assoc step-execution
                              :step-execution/logs
                              (->> (model/query '{:find [[(pull ?log [:* {:execution-log/level [:db/ident]}]) ...]]
                                                  :in [$ ?step-execution-id ?instance-id]
                                                  :where [[?log :execution-log/step-execution-id ?step-execution-id]
                                                          [?log :execution-log/agent ?agent]
                                                          [?agent :agent/instance-id ?instance-id]]}
                                                (:step-execution/step-execution-id step-execution)
                                                (get-in je [:job-execution/agent :agent/instance-id] ""))
                                   (sort-by :execution-log/date compare)))) %))))

(defn find-step-execution [instance-id step-execution-id]
  (model/query
   '{:find [?step-execution .]
     :in [$ ?instance-id ?step-execution-id]
     :where [[?job-execution :job-execution/agent ?agent]
             [?agent :agent/instance-id ?instance-id]
             [?job-execution :job-execution/step-executions ?step-execution]
             [?step-execution :step-execution/step-execution-id ?step-execution-id]]}
   instance-id step-execution-id))


(defn save-execution [id execution]
  (log/debug "progress update: " id execution)
  (let [notifications (some-> (model/query '{:find [(pull ?job [{:job/status-notifications
                                                                 [{:status-notification/batch-status [:db/ident]}
                                                                  :status-notification/type]}]) .]
                                             :in [$ ?id]
                                             :where [[?job :job/executions ?id]]} id)
                              :job/status-notifications)]
    (->> notifications
         (filter #(= (get-in % [:status-notification/batch-status :db/ident]) (:batch-status execution)))
         (map #(notification/send (:status-notification/type %)
                                  execution))
         doall))
  (model/transact [(merge {:db/id id
                           :job-execution/batch-status (:batch-status execution)}
                          (when-let [start-time (:start-time execution)]
                            {:job-execution/start-time start-time})
                          (when-let [end-time (:end-time execution)]
                            {:job-execution/end-time end-time}))]))

(defn edn->datoms
  "Convert a format from EDN to datom."
  [job job-id]
  (let [datoms (atom [])
        step-refs (doall
                   (for [step (->> (:job/components job)
                                   (filter #(find % :step/name)))]
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
                         id)))
        job-id (or job-id (d/tempid :db.part/user)) ]
    (concat [{:db/id job-id
              :job/name (:job/name job)
              :job/restartable? (get job :job/restartable? true) 
              :job/edn-notation (pr-str job)
              :job/steps step-refs
              :job/exclusive? (get job :job/exclusive? false)}]
            (when-let [time-monitor (:job/time-monitor job)]
              [(assoc time-monitor :db/id #db/id[db.part/user -1])
               [:db/add job-id :job/time-monitor #db/id[db.part/user -1]]])
            @datoms)))
