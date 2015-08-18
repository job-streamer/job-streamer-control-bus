(ns job-streamer.control-bus.job
  (:require [datomic.api :as d]
            [clojure.tools.logging :as log]
            (job-streamer.control-bus (model :as model)
                                      (notification :as notification)))
  (:import [org.jsoup Jsoup]))

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
                   (map #(->> (first %)
                              (model/pull '[:*
                                            {(limit :job/executions 99999)
                                             [:db/id
                                              :job-execution/create-time
                                              :job-execution/start-time
                                              :job-execution/end-time
                                              :job-execution/exit-status
                                              {:job-execution/batch-status [:db/ident]}]}
                                            {:job/schedule
                                             [:db/id :schedule/cron-notation :schedule/active?]}])))
                   (map (fn [job]
                          (update-in job [:job/executions]
                                     (fn [executions]
                                       (->> executions
                                            (sort-by :job-execution/create-time #(compare %2 %1))
                                            (take 100)))) ))
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
                                       :job-execution/exit-status
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
  (let [job (model/query '{:find [(pull ?job [:job/name
                                              {:job/steps [:step/name]}
                                              {:job/status-notifications
                                               [{:status-notification/batch-status [:db/ident]}
                                                :status-notification/exit-status
                                                :status-notification/type]}]) .]
                           :in [$ ?id]
                           :where [[?job :job/executions ?id]]} id)]
    (->> (:job/status-notifications job)
         (filter #(or (= (get-in % [:status-notification/batch-status :db/ident])
                         (:batch-status execution))
                      (= (:status-notification/exit-status %) (:exit-status execution))))
         (map #(notification/send (:status-notification/type %)
                                  (assoc execution :job-name (:job/name job))))
         doall))
  (model/transact [(merge {:db/id id
                           :job-execution/batch-status (:batch-status execution)}
                          (when-let [exit-status (:exit-status execution)]
                            {:job-execution/exit-status exit-status})
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

(declare xml->components)

(defn xml->batchlet [batchlet]
  (merge {}
         (when-let [ref (not-empty (.attr batchlet "ref"))]
           {:batchlet/ref ref})))

(defn xml->chunk [chunk]
  (merge {}
         (when-let [checkpoint-policy (not-empty (.attr chunk "checkpoint-policy"))]
           {:chunk/checkpoint-policy checkpoint-policy})
         (when-let [item-count (not-empty (.attr chunk "item-count"))]
           {:chunk/item-count item-count})
         (when-let [time-limit (not-empty (.attr chunk "time-limit"))]
           {:chunk/time-limit time-limit})
         (when-let [skip-limit (not-empty (.attr chunk "skip-limit"))]
           {:chunk/skip-limit skip-limit})
         (when-let [retry-limit (not-empty (.attr chunk "retry-limit"))]
           {:chunk/retry-limit retry-limit})
         (when-let [item-reader (first (. chunk select "> reader[ref]"))]
           {:chunk/reader {:reader/ref (.attr item-reader "ref")}})
         (when-let [item-processor (first (. chunk select "> processor[ref]"))]
           {:chunk/processor {:processor/ref (.attr item-processor "ref")}})
         (when-let [item-writer (first (. chunk select "> writer[ref]"))]
           {:chunk/writer {:writer/ref (.attr item-writer "ref")}})))

(defn xml->step [step]
  (merge {}
         (when-let [id (.attr step "id")]
           {:step/name id})
         (when-let [start-limit (not-empty (.attr step "start-limit"))]
           {:step/start-limit start-limit})
         (when-let [allow-start-if-complete (not-empty (.attr step "allow-start-if-complete"))]
           {:step/allow-start-if-complete? allow-start-if-complete})
         (when-let [next (not-empty (.attr step "next"))]
           {:step/next next})
         (when-let [chunk (first (. step select "> chunk"))]
           {:step/chunk (xml->chunk chunk)})
         (when-let [batchlet (first (. step select "> batchlet"))]
           {:step/batchlet (xml->batchlet batchlet)})))

(defn xml->flow [flow]
  (merge {}
         (when-let [id (.attr flow "id")]
           {:flow/name id})
         (when-let [next (not-empty (.attr flow "next"))]
           {:flow/next next})
         (when-let [components (not-empty (.select flow "> step"))]
           {:flow/components (xml->components components)})))

(defn xml->split [split]
  (merge {}
         (when-let [id (.attr split "id")]
           {:split/name id})
         (when-let [split (not-empty (.attr split "next"))]
           {:split/next next})
         (when-let [components (not-empty (.select split "> flow"))]
           {:split/components (xml->components components)})))

(defn xml->components [job]
  (->> (for [component (. job select "> step,flow,split,decision")]
         (case (.tagName component)
           "step" (xml->step component)
           "flow" (xml->flow component)
           "split" (xml->split component)
           "decision" (throw (Exception. "Unsupported `decision`"))))
       vec))

(defn xml->edn
  "Convert a format from XML to edn."
  [xml]
  (let [doc (Jsoup/parse xml)
        job (. doc select "job")]
    {:job/name (.attr job "id")
     :job/components (xml->components job)}))
