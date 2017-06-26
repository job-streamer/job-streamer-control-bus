(ns job-streamer.control-bus.component.jobs
  (:require [clojure.tools.logging :as log]
            [clojure.set :as set]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clojure.edn :as edn]
            [com.stuartsierra.component :as component]
            [bouncer.core :as b]
            [bouncer.validators :as v]
            [liberator.core :as liberator]
            [clojure.string :as str]
            [clj-time.format :as f]
            [liberator.representation :refer [ring-response]]
            [ring.util.response :refer [response content-type header]]
            (job-streamer.control-bus [notification :as notification]
                                      [validation :refer [validate]]
                                      [util :refer [parse-body edn->datoms to-int]])
            (job-streamer.control-bus.component [datomic :as d]
                                                [agents  :as ag]
                                                [scheduler :as scheduler]
                                                [apps :as apps]))
  (:import [java.util Date]
           [org.jsoup Jsoup]
           [org.jsoup.nodes Element Node]
           [org.jsoup.parser Tag Parser]
           [net.unit8.job_streamer.control_bus.bpmn BpmnParser]))

(defn find-latest-execution
  "Find latest from given executions."
  [executions]
  (when executions
    (->> executions
         (sort #(compare (:job-execution/create-time %2)
                         (:job-execution/create-time %1)))
         first)))

(defn- find-next-execution
  "Find next execution from a scheduler.
  If a job isn't scheduled, it returns nil."
  [{:keys [scheduler]} job]
  (when (:job/schedule job)
    (if-let [next-start (first (scheduler/fire-times scheduler (:db/id job)))]
      {:job-execution/start-time next-start})))

(defn extract-job-parameters [job]
  (when-let [bpmn (:job/bpmn-xml-notation job)]
    (let [jobxml (Jsoup/parse bpmn "" (Parser/xmlParser))
          dynamic-properties (.select jobxml "bpmn|extensionElements > camunda|properties > camunda|property[value~=#\\{jobParameters\\['[\\w\\-]+'\\]\\}]")]
      (doall (->> dynamic-properties
                  (map #(->> (.attr % "value")
                             (re-seq #"jobParameters\['([\w\-]+)'\]")
                             (map second)))
                  flatten
                  (apply hash-set))))))

(defn find-undispatched [{:keys [datomic]}]
  (d/query
   datomic
   '{:find [?job-execution ?job-obj ?param-map]
     :where [[?job :job/executions ?job-execution]
             [?job-execution :job-execution/job-parameters ?parameter]
             [?job :job/bpmn-xml-notation ?job-obj]
             (or [?job-execution :job-execution/batch-status :batch-status/undispatched]
                 [?job-execution :job-execution/batch-status :batch-status/unrestarted])
             [(clojure.edn/read-string ?parameter) ?param-map]]}))

(defn find-by-name [{:keys [datomic]} app-name job-name]
  (d/query
   datomic
   '{:find [[?app ?job]]
     :in [$ ?app-name ?job-name]
     :where [[?app :application/name ?app-name]
             [?app :application/jobs ?job]
             [?job :job/name ?job-name]]}
   app-name job-name))

(defn- parse-query-since [q]
  (let [since (.substring q (count "since:"))]
    (when (b/valid? {:since since}
                    :since [[v/datetime (:date f/formatters)]])
      {:since (some->> since (f/parse (:date f/formatters)) .toDate)})))

(defn- parse-query-until [q]
  (let [until (.substring q (count "until:"))
        parse-date-fn (partial f/parse (:date f/formatters))]
    (when (b/valid? {:until until}
                    :until [[v/datetime (:date f/formatters)]])
      {:until (some-> until
                      parse-date-fn
                      (.plusDays 1)
                      .toDate)})))

(defn- parse-query-exit-status [q]
  (let [exit-status (some-> q (.substring (count "exit-status:")) .toUpperCase)]
    (when (b/valid? {:exit-status exit-status}
                    :exit-status v/required)
      {:exit-status exit-status})))

(defn- parse-query-batch-status [q]
  (let [batch-status (some-> q (.substring (count "batch-status:")) .toLowerCase)]
    (when (b/valid? {:batch-status batch-status}
                    :batch-status [[v/member #{
                                              ;jsr352
                                               "abandoned"
                                               "completed"
                                               "failed"
                                               "started"
                                               "starting"
                                               "stopped"
                                               "stopping"
                                              ;spring batch original
                                               "unknown"
                                              ;job-streamer original
                                               "registered"
                                               "unrestarted"
                                               "undispatched"
                                               "queued"}]])
      {:batch-status (keyword "batch-status" batch-status)})))

(defn parse-query [query]
  (when (not-empty query)
    (->> (str/split query #"\s")
         (map #(cond
                 (.startsWith % "since:") (parse-query-since %)
                 (.startsWith % "until:") (parse-query-until %)
                 (.startsWith % "exit-status:") (parse-query-exit-status %)
                 (.startsWith % "batch-status:") (parse-query-batch-status %)
                 :default {:job-name [%]}))
         (apply merge-with concat {:job-name nil}))))

(defn- decide-sort-order [asc-or-desc v1 v2]
  (if (= :asc asc-or-desc)
    (compare v1 v2)
    (compare v2 v1)))

(defn- parse-sort-order-component [query]
  (let [split (str/split query #":")
        name (first split)
        sort-order (some-> split second .toLowerCase)]
    (when (and
            (b/valid? {:sort-order sort-order}
                      :sort-order [[v/member #{
                                                "asc"
                                                "desc"}]])
            (b/valid? {:name name}
                      :name [[v/member #{
                                          "name"
                                          "last-execution-started"
                                          "last-execution-duration"
                                          "last-execution-status"
                                          "next-execution-start"
                                          }]]))
      {(keyword name) (keyword sort-order)})))

(defn parse-sort-order [query]
  (when (not-empty query)
    (->> (str/split query #",")
         (map #(parse-sort-order-component %))
         (apply merge))))

(defn sort-by-map [sort-order job-list]
  (if (empty? sort-order)
    job-list
    (loop [sort-order-vector (reverse (seq sort-order))
           sorted-result job-list]
      (if-let [[sort-key direction] (first sort-order-vector)]
        (recur (rest sort-order-vector)
               (cond->> sorted-result
                        (= :name sort-key)
                        (sort-by :job/name (fn [v1 v2] (decide-sort-order direction v1 v2)))

                        (= :last-execution-started sort-key)
                        (sort-by (fn [m] (get-in m [:job/latest-execution :job-execution/start-time]))
                                 (fn [v1 v2] (decide-sort-order direction v1 v2)))

                        (= :last-execution-status sort-key)
                        (sort-by (fn [m] (get-in m [:job/latest-execution :job-execution/batch-status :db/ident]))
                                 (fn [v1 v2] (decide-sort-order direction v1 v2)))

                        (= :last-execution-duration sort-key)
                        (sort-by (fn [m]
                                   (if (get-in m [:job/latest-execution :job-execution/end-time])
                                     (-
                                      (-> m (get-in  [:job/latest-execution :job-execution/end-time])  c/from-date c/to-long)
                                      (-> m (get-in  [:job/latest-execution :job-execution/start-time])  c/from-date c/to-long))
                                     nil))
                                 (fn [v1 v2] (decide-sort-order direction v1 v2)))

                        (= :next-execution-start sort-key)
                        (sort-by (fn [m] (get-in m [:job/next-execution :job-execution/start-time]))
                                 (fn [v1 v2] (decide-sort-order direction v1 v2)))))
        sorted-result))))

(defn find-all [{:keys [datomic]} app-name query]
  (let [qmap (parse-query query)
        search-by-executions? (or (:since qmap) (:until qmap) (:exit-status qmap) (:batch-status qmap))
        base-query '{:find [?job]
                     :in [$ ?app-name [?job-name-condition ...] ?since-condition ?until-condition ?exit-status-condition ?batch-status-condition]
                     :where [[?app :application/name ?app-name]
                             [?app :application/jobs ?job]]}
        jobs (cond-> (d/query datomic
                              (cond-> base-query
                                (not-empty (:job-name qmap))
                                (update-in [:where] conj
                                           '[?job :job/name ?job-name]
                                           '[(.contains ^String ?job-name ?job-name-condition)])

                                search-by-executions?
                                (#(-> %
                                      (update-in [:find] conj
                                                 '?job-executions)
                                      (update-in [:where] conj
                                                 '[?job :job/executions ?job-executions]
                                                 '[?job-executions :job-execution/create-time ?create-time]
                                                 '[(max ?create-time)]
                                                 '[?job-executions :job-execution/exit-status ?exit-status]
                                                 '[?job-executions :job-execution/end-time ?end-time]
                                                 '[?job-executions :job-execution/start-time ?start-time])))

                                (:since qmap)
                                (update-in [:where] conj
                                           '[(>= ?start-time ?since-condition)])

                                (:until qmap)
                                (update-in [:where] conj
                                           '[(< ?end-time ?until-condition)])

                                (:exit-status qmap)
                                (update-in [:where] conj
                                           '[(.contains ^String ?exit-status ?exit-status-condition)])
                                (:batch-status qmap)
                                (update-in [:where] conj
                                           '[?job-executions :job-execution/batch-status  ?batch-status-condition]))
                              app-name
                              ;if argument is nil or empty vector datomic occurs error
                              (or (not-empty (:job-name qmap)) [""])
                              (:since qmap "")
                              (:until qmap "")
                              (:exit-status qmap "")
                              (:batch-status qmap ""))
                     search-by-executions?
                     ((fn [jobs] (let [latest-executions
                                       (->> (d/query datomic
                                                     '{:find [?job (max ?job-executions)]
                                                       :where [[?app :application/jobs ?job]
                                                               [?job :job/executions ?job-executions]]})
                                            (map second) set)]
                                   (filter #(latest-executions (second %)) jobs)))))]
    (->> jobs
         (map #(->> (first %)
                    (d/pull datomic
                            '[:*
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
                                  (take 100))))))
         vec)))

(defn find-executions [{:keys [datomic]} app-name job-name & [offset limit]]
  (let [executions (d/query datomic
                            '{:find [?job-execution ?create-time]
                              :in [$ ?app-name ?job-name]
                              :where [[?job-execution :job-execution/create-time ?create-time]
                                      [?app :application/name ?app-name]
                                      [?app :application/jobs ?job]
                                      [?job :job/name ?job-name]
                                      [?job :job/executions ?job-execution]]}
                            app-name job-name)]
    {:results (->> executions
                   (sort-by second #(compare %2 %1))
                   (drop (dec offset))
                   (take limit)
                   (map #(d/pull datomic
                                 '[:db/id
                                   :job-execution/execution-id
                                   :job-execution/create-time
                                   :job-execution/start-time
                                   :job-execution/end-time
                                   :job-execution/job-parameters
                                   :job-execution/exit-status
                                   {:job-execution/batch-status [:db/ident]
                                    :job-execution/agent
                                    [:agent/instance-id :agent/name]}]
                                 (first %)))

                   vec)
     :hits    (count executions)
     :offset  offset
     :limit   limit}))

(defn find-execution [{:keys [datomic]} job-execution]
  (let [je (d/pull datomic
                   '[:*
                     {:job-execution/agent [:agent/instance-id :agent/name]}
                     {:job-execution/step-executions
                      [:*
                       {:step-execution/batch-status [:db/ident]}
                       :step-execution/step-name]}] job-execution)]
    (update-in je [:job-execution/step-executions]
               #(map (fn [step-execution]
                       (assoc step-execution
                              :step-execution/logs
                              (->> (d/query datomic
                                            '{:find [[(pull ?log [:* {:execution-log/level [:db/ident]}]) ...]]
                                              :in [$ ?step-execution-id ?instance-id]
                                              :where [[?log :execution-log/step-execution-id ?step-execution-id]
                                                      [?log :execution-log/agent ?agent]
                                                      [?agent :agent/instance-id ?instance-id]]}
                                            (:step-execution/step-execution-id step-execution)
                                            (get-in je [:job-execution/agent :agent/instance-id] ""))
                                   (sort-by :execution-log/date compare)))) %))))

(defn find-step-execution [{:keys [datomic]} instance-id step-execution-id]
  (d/query datomic
           '{:find [?step-execution .]
             :in [$ ?instance-id ?step-execution-id]
             :where [[?job-execution :job-execution/agent ?agent]
                     [?agent :agent/instance-id ?instance-id]
                     [?job-execution :job-execution/step-executions ?step-execution]
                     [?step-execution :step-execution/step-execution-id ?step-execution-id]]}
           instance-id step-execution-id))


(defn save-execution [{:keys [datomic] :as jobs} id execution]
  (log/debug "progress update: " id execution)
  (if (-> jobs :test-executions deref (get id))
    (swap! (:test-executions jobs) update-in [id] merge {:batch-status (:batch-status execution)
                                                         :execution-id (:execution-id execution)})
    (let [job (d/query datomic
                       '{:find [(pull ?job [:job/name
                                            {:job/status-notifications
                                             [{:status-notification/batch-status [:db/ident]}
                                              :status-notification/exit-status
                                              :status-notification/type]}]) .]
                         :in [$ ?id]
                         :where [[?job :job/executions ?id]]} id)]
      (->> (:job/status-notifications job)
           (filter #(or (= (get-in % [:status-notification/batch-status :db/ident])
                           (:batch-status execution))
                        (and (:exit-status execution) (= (:status-notification/exit-status %) (:exit-status execution)))))
           (map #(notification/send (:status-notification/type %)
                                    (assoc execution :job-name (:job/name job))))
           doall)
      (d/transact datomic
                  [(merge {:db/id id
                           :job-execution/batch-status (:batch-status execution)}
                          (when-let [exit-status (:exit-status execution)]
                            {:job-execution/exit-status exit-status})
                          (when-let [start-time (:start-time execution)]
                            {:job-execution/start-time start-time})
                          (when-let [end-time (:end-time execution)]
                            {:job-execution/end-time end-time})
                          (when-let [step-executions (and (empty? (d/query datomic
                                                                           '{:find [?step-executions]
                                                                             :in [$]
                                                                             :where [[?step-executions :job-execution/step-executions ?job-execution-id]]} id))
                                                          (:step-executions execution))]
                            {:job-execution/step-executions (map (fn [m]
                                                                   (->> m
                                                                        (map #(vector (keyword "step-execution" (name (key %))) (val %)))
                                                                        (into {:db/id (d/tempid :db.part/user)})))
                                                                 step-executions)}))]))))

(defn save-status-notification
  "Save a given status notification."
  [{:keys [datomic] :as jobs} job-id settings]
  (let [status-notification-id (d/tempid :db.part/user)
        tempids (-> (d/transact
                     datomic
                     [[:db/add job-id
                       :job/status-notifications status-notification-id]
                      (merge {:db/id status-notification-id
                              :status-notification/type (:status-notification/type settings)}
                             (when-let [batch-status (:status-notification/batch-status settings)]
                               {:status-notification/batch-status batch-status})
                             (when-let [exit-status (:status-notification/exit-status settings)]
                               {:status-notification/exit-status exit-status}))])
                    :tempids)]
    {:db/id (d/resolve-tempid datomic tempids status-notification-id)}))

(defn- append-schedule [scheduler job-id executions schedule]
  (if (:schedule/active? schedule)
    (let [schedules (scheduler/fire-times scheduler job-id)]
      (apply conj executions
             (map (fn [sch]
                    {:job-execution/start-time sch
                     :job-execution/end-time (Date. (+ (.getTime sch) (* 5 60 1000)))
                     :job-execution/batch-status {:db/ident :batch-status/registered}}) schedules)))
    executions))

(defn include-job-attrs [{:keys [datomic scheduler] :as jobs} with-params job-list]
  (->> job-list
       (map (fn [{job-name :job/name
                  executions :job/executions
                  schedule :job/schedule :as job}]
              (merge {:job/name job-name}
                     (when (with-params :execution)
                       {:job/executions (append-schedule scheduler (:db/id job) executions schedule)
                        :job/latest-execution (find-latest-execution executions)
                        :job/next-execution   (find-next-execution jobs job)})
                     (when (with-params :schedule)
                       {:job/schedule schedule})
                     (when (with-params :notation)
                       {:job/bpmn-xml-notation (:job/bpmn-xml-notation job)
                        :job/svg-notation (:job/svg-notation job)})
                     (when (with-params :settings)
                       (merge {:job/exclusive? (get job :job/exclusive? false)}
                              (when-let [time-monitor (get-in job [:job/time-monitor :db/id])]
                                {:job/time-monitor (d/pull datomic
                                                           '[:time-monitor/duration
                                                             {:time-monitor/action [:db/ident]}
                                                             :time-monitor/notification-type] time-monitor)})
                              (when-let [status-notifications (:job/status-notifications job)]
                                {:job/status-notifications (->> status-notifications
                                                                (map (fn [sn]
                                                                       (d/pull datomic
                                                                               '[{:status-notification/batch-status [:db/ident]}
                                                                                 :status-notification/exit-status
                                                                                 :status-notification/type] (:db/id sn))))
                                                                vec)}))))))
       vec))

(defn parse-with-params
  "Parse `with` parameter for separating by comma"
  [with-params]
  (->> (clojure.string/split
        (or (not-empty with-params) "execution")
        #"\s*,\s*")
       (map keyword)
       set))


(defn convert-to-test-job [jobxml-str]
 (let [jobxml (Jsoup/parse  jobxml-str "" (Parser/xmlParser))
       steps (.select jobxml "step")
       batchlets (.select jobxml "step > batchlet")
       chunks (.select jobxml "step > chunk")]
     (.remove batchlets)
     (.remove chunks)
   (doall
     (map #(.appendChild % (some-> (Tag/valueOf "batchlet") (Element. "") (.attr "ref" "org.jobstreamer.batch.TestBatchlet"))) steps))
   (.toString jobxml)))

(defn make-job [job-bpmn-xml]
  (log/info "make job-xml form bpmn" \newline job-bpmn-xml)
  (str "<?xml version=\"1.0\" encoding=\"UTF-8\"?> " \newline (some-> (new BpmnParser) (.parse job-bpmn-xml) .toString)))

(defn search-state-id [jobs execution-id]
  (loop[times 1]
    (Thread/sleep 1000)
    (let [state (->> @(:test-executions jobs) (filter #(= (-> % val :execution-id) execution-id)) first)]
      (if (empty? state)
        (recur (inc times))
        (key state)))))


(defn write-error-message-on-state [jobs execution-id message exception]
  (let [state-id (search-state-id jobs execution-id)]
    (swap! (:test-executions jobs) assoc state-id {:log-message message
                                                   :log-exception exception
                                                   :batch-status (-> @(:test-executions jobs) (get state-id) :batch-status)})))

(defn dispatch-test-job [{:keys [datomic agents apps] :as jobs} ctx]
  (let [job-bpmn-xml (get-in ctx [:edn :bpmn])
        agt (ag/find-agent agents)
        state-id (->> jobs :test-executions deref keys (into [0]) (apply max) inc)]
    (swap! (:test-executions jobs) assoc state-id {:batch-status :batch-status/undispatched})
    (if (nil? agt)
      (swap! (:test-executions jobs) assoc state-id {:batch-status :batch-status/failed
                                                     :log-message "No agent available"
                                                     :log-exeption "Please run agent for exetute"})
      (ag/execute-job agt
                    {:request-id state-id
                     :class-loader-id (:application/class-loader-id
                                        (apps/find-by-name apps "default"))
                     :job (-> job-bpmn-xml make-job convert-to-test-job)
                     :restart? false}))
    {:state-id state-id}))

(defn list-resource
  [{:keys [datomic scheduler] :as jobs} app-name & {:keys [download?] :or {download? false}}]
  (liberator/resource
   :available-media-types ["application/edn" "application/json"]
   :allowed-methods [:get :post]
   :malformed? (fn [ctx]
                 (validate (parse-body ctx)
                           :job/name [v/required [v/matches #"^[\w\-]+$"]]))
   :exists? (fn [{{job-name :job/name} :edn :as ctx}]
              (if (#{:post} (get-in ctx [:request :request-method]))
                (when-let [[_ job-id] (find-by-name jobs app-name job-name)]
                  {:job-id job-id})
                true))
   :allowed? (fn [{{:keys [request-method identity]} :request}]
               (let [permissions (:permissions identity)]
                 (condp = request-method
                   :get (:permission/read-job permissions)
                   :post (:permission/create-job permissions)
                   false)))
   :post! (fn [{job :edn posted-job-id :job-id}]
            (let [datoms (edn->datoms job posted-job-id)
                  job-id (:db/id (first datoms))]
              (let [resolved-job-id
                    (or posted-job-id
                        (d/resolve-tempid
                          datomic
                          (:tempids
                            (d/transact datomic
                                        (conj datoms
                                              [:db/add [:application/name app-name] :application/jobs job-id])))
                          job-id))]
                (doseq [notification (:job/status-notifications job)]
                  (save-status-notification jobs resolved-job-id notification))
                (d/transact datomic
                                   [{:db/id resolved-job-id
                                     :job/exclusive? (:job/exclusive? job false)}])
                (when-let [schedule (:job/schedule job)]
                  (scheduler/schedule
                   scheduler resolved-job-id
                   (:schedule/cron-notation schedule)
                   nil)) ;; FIXME A Calendar cannnot be set here.
                job)))
   :handle-ok (fn [{{{query :q with-params :with sort-order :sort-by
                      :keys [limit offset]} :params} :request}]
                (let [job-list (->> (find-all jobs app-name query))
                      res (->> job-list
                               (include-job-attrs jobs (parse-with-params with-params))
                               (sort-by-map (parse-sort-order sort-order))
                               (drop (dec (to-int offset 0)))
                               (take (to-int limit (if download? 99999 20)))
                               vec)]
                  (if download?
                    (-> res
                        pr-str
                        response
                        (content-type "application/force-download")
                        (header "Content-disposition" "attachment; filename=\"jobs.edn\"")
                        (ring-response))
                    {:results res
                     :hits    (count job-list)
                     :limit   (to-int limit 20)
                     :offset  (to-int offset 0)})))))

(defn entry-resource [{:keys [datomic scheduler] :as jobs} app-name job-name]
  (liberator/resource
   :available-media-types ["application/edn" "application/json"]
   :allowed-methods [:get :put :delete]
   :malformed? #(parse-body %)
   :exists? (when-let [[app-id job-id] (find-by-name jobs app-name job-name)]
              {:app-id app-id
               :job-id job-id})
   :allowed? (fn [{{:keys [request-method identity]} :request}]
               (let [permissions (:permissions identity)]
                 (condp = request-method
                   :get (:permission/read-job permissions)
                   :put (:permission/update-job permissions)
                   :delete (:permission/delete-job permissions)
                   false)))
   :put! (fn [{job :edn job-id :job-id}]
           (d/transact datomic (edn->datoms job job-id))
           (let [schedule (d/pull datomic
                                  '[{:job/schedule
                                     [:schedule/cron-notation
                                      {:schedule/calendar
                                       [:calendar/name]}]}] job-id)]
             (when-let [cron-notation (:schedule/cron-notation (:job/schedule schedule))]
               (scheduler/unschedule scheduler job-id)
               (scheduler/schedule scheduler job-id cron-notation (:calendar/name (:schedule/calendar (:job/schedule schedule))))))) ; Because job execute by job name
   :delete! (fn [{job-id :job-id app-id :app-id}]
              (scheduler/unschedule scheduler job-id)
              (d/transact datomic
                          [[:db.fn/retractEntity job-id]
                           [:db/retract app-id :application/jobs job-id]]))
   :handle-ok (fn [ctx]
               (let [job (d/pull datomic
                                 '[:*
                                   {(limit :job/executions 99999)
                                    [:db/id
                                     :job-execution/start-time
                                     :job-execution/end-time
                                     :job-execution/create-time
                                     :job-execution/exit-status
                                     {:job-execution/batch-status [:db/ident]}
                                     {:job-execution/agent [:agent/name :agent/instance-id]}]}
                                   {:job/schedule [:schedule/cron-notation :schedule/active?]}]
                                 (:job-id ctx))
                     total (count (:job/executions job))
                     success (->> (:job/executions job)
                                  (filter #(= (get-in % [:job-execution/batch-status :db/ident])
                                              :batch-status/completed))
                                  count)
                     failure (->> (:job/executions job)
                                  (filter #(= (get-in % [:job-execution/batch-status :db/ident])
                                              :batch-status/failed))
                                  count)
                     average (if (= success 0) 0
                               (/ (->> (:job/executions job)
                                       (filter #(= (get-in % [:job-execution/batch-status :db/ident])
                                                   :batch-status/completed))
                                       (map #(- (.getTime (:job-execution/end-time %))
                                                (.getTime (:job-execution/start-time %))))
                                       (reduce +))
                                  success))]
                 (-> job
                     (assoc :job/stats {:total total :success success :failure failure :average average}
                       :job/latest-execution (find-latest-execution (:job/executions job))
                       :job/next-execution   (find-next-execution jobs job)
                       :job/dynamic-parameters (extract-job-parameters job))
                     (dissoc :job/executions))))))


(defn job-settings-resource [{:keys [datomic] :as jobs} app-name job-name & [cmd]]
  (liberator/resource
  :available-media-types ["application/edn" "application/json"]
  :allowed-methods [:get :delete :put]
  :malformed? #(parse-body %)
  :exists? (when-let [[app-id job-id] (find-by-name jobs app-name job-name)]
             {:app-id app-id
              :job-id job-id})
   :allowed? (fn [{{:keys [request-method identity]} :request}]
               (let [permissions (:permissions identity)]
                 (condp = request-method
                   :get (:permission/read-job permissions)
                   :put (:permission/update-job permissions)
                   :delete (:permission/delete-job permissions)
                   false)))
  :put! (fn [{settings :edn job-id :job-id}]
          (case cmd
            :exclusive (d/transact datomic
                                   [{:db/id job-id
                                     :job/exclusive? true}])

            :status-notification
            (if-let [id (:db/id settings)]
              (d/transact datomic
                          [[:db/retract job-id :job/status-notifications id]])
              (save-status-notification jobs job-id settings))

            :time-monitor
            (d/transact datomic
                        [(merge {:db/id #db/id[db.part/user -1]} settings)
                         {:db/id job-id :job/time-monitor #db/id[db.part/user -1]}])))

  :delete! (fn [{settings :edn job-id :job-id}]
             (case cmd
               :exclusive (d/transact datomic [{:db/id job-id
                                                :job/exclusive? false}])
               :time-monitor
               (when-let [time-monitor-id (some-> (d/pull datomic
                                                          '[:job/time-monitor] job-id)
                                                  :job/time-monitor
                                                  :db/id)]
                 (d/transact datomic
                             [[:db/retract job-id
                               :job/time-monitor time-monitor-id]
                              [:db.fn/retractEntity time-monitor-id]]))))

  :handle-created (fn [ctx]
                    (select-keys ctx [:db/id]))

  :handle-ok (fn [ctx]
               (let [settings (d/pull datomic
                                      '[:job/exclusive?
                                        {:job/time-monitor
                                         [:time-monitor/duration
                                          {:time-monitor/action [:db/ident]}
                                          :time-monitor/notification-type]}
                                        {:job/status-notifications
                                         [:db/id
                                          {:status-notification/batch-status [:db/ident]}
                                          :status-notification/exit-status
                                          :status-notification/type]}]
                                      (:job-id ctx))]
                 (-> settings
                     (update-in
                      [:job/status-notifications]
                      (fn [notifications]
                        (map #(assoc %
                                     :status-notification/batch-status
                                     (get-in % [:status-notification/batch-status :db/ident])) notifications)))
                     (update-in [:job/time-monitor]
                                (fn [time-monitor]
                                  (when-let [action (get-in time-monitor [:time-monitor/action :db/ident])]
                                    (assoc time-monitor :time-monitor/action action)))))))))

(defn- execute-job [{:keys [datomic scheduler] :as jobs} app-name job-name ctx]
  (log/debug "execute job " job-name)
  (when-let [[app-id job-id] (find-by-name jobs app-name job-name)]
    (let [execution-id (d/tempid :db.part/user)
          tempids (-> (d/transact
                       datomic
                       [{:db/id execution-id
                         :job-execution/batch-status :batch-status/undispatched
                         :job-execution/create-time (java.util.Date.)
                         :job-execution/job-parameters (pr-str (or (:edn ctx) {}))}
                        [:db/add job-id :job/executions execution-id]])
                      :tempids)]
      (log/debug "set execution-id " (-> tempids vals first))
      (when-let [time-monitor (d/pull datomic
                                      '[{:job/time-monitor
                                         [:time-monitor/duration
                                          {:time-monitor/action [:db/ident]}]}]
                                      job-id)]
        (log/debug "set time-monitor")
        (scheduler/time-keeper
         scheduler
         (d/resolve-tempid datomic tempids execution-id)
         (get-in time-monitor [:job/time-monitor :time-monitor/duration])
         (get-in time-monitor [:job/time-monitor :time-monitor/action :db/ident]))))))

(defn executions-resource [{:keys [datomic] :as jobs} app-name job-name]
  (liberator/resource
   :available-media-types ["application/edn" "application/json"]
   :allowed-methods [:get :post :delete]
   :malformed? #(parse-body %)
   :exists? (when-let [[app-id job-id] (find-by-name jobs app-name job-name)]
              {:job (d/pull datomic
                            '[:job/exclusive?
                              {:job/executions
                               [:job-execution/create-time
                                {:job-execution/batch-status [:db/ident]}]}] job-id)})
   :post-to-existing? (fn [ctx]
                        (when (#{:put :post} (get-in ctx [:request :request-method]))
                           (let [job (d/pull datomic
                                             '[:*
                                               {:job/executions
                                                [:db/id
                                                 :job-execution/create-time
                                                 :job-execution/end-time]}
                                               {:job/schedule [:schedule/cron-notation :schedule/active?]}]
                                             (second (find-by-name jobs app-name job-name)))
                                 last-execution (-> job :job/executions find-latest-execution)]
                             (not (and (:job/exclusive? (:job ctx))
                                       last-execution
                                       (not (:job-execution/end-time last-execution)))))))
   :put-to-existing? (fn [ctx]
                       (#{:put :post} (get-in ctx [:request :request-method])))
   :post-to-missing? (fn [ctx] (find-by-name jobs app-name job-name))
   :conflict? (fn [{job :job}]
                (if-let [last-execution (some->> (:job/executions job)
                                                 (sort #(compare (:job-execution/create-time %2) (:job-execution/create-time %1)))
                                                 first)]
                  (contains? #{:batch-status/undispatched
                               :batch-status/queued
                               :batch-status/starting
                               :batch-status/started
                               :batch-status/stopping}
                             (get-in last-execution [:job-execution/batch-status :db/ident]))
                  false))
   :allowed? (fn [{{:keys [request-method identity]} :request}]
               (let [permissions (:permissions identity)]
                 (condp = request-method
                   :get (:permission/read-job permissions)
                   :post (:permission/execute-job permissions)
                   :delete (:permission/delete-job permissions)
                   false)))
   :put! #(execute-job jobs app-name job-name %)
   :post! #(execute-job jobs app-name job-name %)
   :delete! (fn [ctx]
              (doall
                (map #(d/transact datomic
                       [[:db.fn/retractEntity (:db/id %)]])
                     (:results (find-executions jobs app-name job-name 0 Integer/MAX_VALUE)))))
   :handle-ok (fn [{{{:keys [offset limit]} :params} :request}]
                (find-executions jobs app-name job-name
                                 (to-int offset 0)
                                 (to-int limit 20)))))

(defn execution-resource [{:keys [agents scheduler datomic] :as jobs} id & [cmd]]
  (liberator/resource
   :available-media-types ["application/edn" "application/json"]
   :allowed-methods [:get :put]
   :malformed? #(parse-body %)
   :exists? (fn [ctx]
              (when-let [execution (d/pull datomic
                                           '[:*
                                             {:job-execution/agent
                                              [:db/id :agent/instance-id]}]
                                           id)]
                (when-let [[app-id job-id] (d/query datomic
                                                    '{:find [[?app ?job]]
                                                      :in [$ ?eid]
                                                      :where [[?job :job/executions ?eid]
                                                              [?app :application/jobs ?job]]}
                                                    id)]
                  {:execution execution
                   :app-id app-id
                   :job-id job-id})))
   :allowed? (fn [{{:keys [request-method identity]} :request}]
               (let [permissions (:permissions identity)]
                 (condp = request-method
                   :get (:permission/read-job permissions)
                   :put (:permission/execute-job permissions)
                   false)))
   :put! (fn [{parameters :edn execution :execution
               job-id :job-id app-id :app-id}]
           (case cmd
             :abandon
             (ag/abandon-execution
              agents
              execution
              :on-success (fn [_]
                            (ag/update-execution-by-id
                             agents
                             id
                             :on-success (fn [response]
                                           (save-execution jobs id response))
                             :on-error (fn [error]
                                         (log/error error)))))

             :stop
             (ag/stop-execution
              agents
              execution
              :on-success (fn [_]
                            (ag/update-execution-by-id
                             agents
                             id
                             :on-success (fn [response]
                                           (save-execution jobs id response)))))

             :restart
             (let [execution-id (d/tempid :db.part/user)
                   tempids (-> (d/transact
                                datomic
                                [{:db/id execution-id
                                  :job-execution/batch-status :batch-status/unrestarted
                                  :job-execution/create-time (java.util.Date.)
                                  :job-execution/agent (:job-execution/agent execution)
                                  :job-execution/job-parameters (pr-str (or parameters {}))}
                                 [:db/add job-id :job/executions execution-id]])
                               :tempids)]
               {:execution-id (d/resolve-tempid datomic tempids execution-id)})

             :alert
             (let [job (d/query datomic
                                '{:find [(pull ?job [:job/name
                                                     {:job/time-monitor
                                                      [:time-monitor/notification-type]}]) .]
                                  :in [$ ?id]
                                  :where [[?job :job/executions ?id]]} id)]
               (notification/send
                (get-in job [:job/time-monitor :time-monitor/notification-type])
                {:job-name (:job/name job)
                 :duration (get-in job [:job/time-monitor :time-monitor/duration])}))
             nil))
   :handle-ok (fn [ctx]
                (find-execution jobs id))))

(defn test-executions-resource [{:keys [agents datomic apps] :as jobs}]
  (liberator/resource
   :available-media-types ["application/edn" "application/json"]
   :allowed-methods [:post]
   :malformed? #(parse-body %)
   :handle-created #(dispatch-test-job jobs %)))

(defn test-execution-resource [{:keys [agents scheduler datomic] :as jobs} id & [cmd]]
  (liberator/resource
   :available-media-types ["application/edn" "application/json"]
   :allowed-methods [:get :delete]
   :exists? (fn [_]
              (get @(:test-executions jobs) id))
   :delete! (fn [_]
              (swap! (:test-executions jobs) dissoc id))
   :handle-ok (fn [ctx]
                (get @(:test-executions jobs) id))))

(defrecord Jobs []
  component/Lifecycle

  (start [component]
    (assoc component :test-executions (atom {})))

  (stop [component]
    (dissoc component :list-resource :entry-resource)))

(defn jobs-component [options]
  (map->Jobs options))
