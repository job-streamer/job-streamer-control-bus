(ns job-streamer.control-bus.api
  "Define resources for web api."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [liberator.core :refer [defresource]]
            [datomic.api :as d]
            [bouncer.validators :as v]
            (job-streamer.control-bus (model :as model)
                                      (apps :as apps)
                                      (job :as job)
                                      (broadcast :as broadcast)
                                      (server :as server)
                                      (scheduler :as scheduler)
                                      (dispatcher :as dispatcher)
                                      (notification :as notification)))
  (:use [clojure.core.async :only [chan put! <! go-loop timeout]]
        [clojure.walk]
        [liberator.representation :only [ring-response]]
        [ring.util.response :only [header]]
        (job-streamer.control-bus (agent :only [find-agent available-agents] :as ag)
                                  (validation :only [validate])))
  (:import [java.util Date]))

(defn- to-int [n default-value]
  (if (nil? n)
    default-value
    (condp = (type n)
      String (Integer/parseInt n)
      Number (int n))))

(defn- body-as-string [ctx]
  (if-let [body (get-in ctx [:request :body])]
    (condp instance? body
      java.lang.String body
      (slurp (io/reader body)))))

(defn- parse-edn [context]
  (when (#{:put :post} (get-in context [:request :request-method]))
    (try
      (if-let [body (body-as-string context)]
        (let [data (edn/read-string body)]
          [false {:edn data}])
        false)
      (catch Exception e
        (log/error e "fail to parse edn.")
        {:message (format "IOException: %s" (.getMessage e))}))))

(defn- find-latest-execution
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
  [job]
  (when (:job/schedule job)
    (if-let [next-start (first (scheduler/fire-times (:db/id job)))]
      {:job-execution/start-time next-start})))

(defn- append-schedule [job-id executions schedule]
  (if (:schedule/active? schedule)
    (let [schedules (scheduler/fire-times job-id)]
      (apply conj executions
             (map (fn [sch]
                    {:job-execution/start-time sch
                     :job-execution/end-time (Date. (+ (.getTime sch) (* 5 60 1000)))
                     :job-execution/batch-status {:db/ident :batch-status/registered}}) schedules)))
    executions))

(defresource stats-resource [app-name]
    :available-media-types ["application/edn"]
    :allowed-methods [:get]
    :handle-ok (fn [ctx]
                 {:agents (count (ag/available-agents))
                  :jobs   (or (model/query '{:find [(count ?job) .]
                                            :in [$ ?app-name]
                                            :where [[?app :application/name ?app-name]
                                                    [?app :application/jobs ?job]]}
                                          app-name) 0)}))

(defresource jobs-resource [app-name]
  :available-media-types ["application/edn"]
  :allowed-methods [:get :post]
  :malformed? #(validate (parse-edn %)
                         :job/name [v/required [v/matches #"^[\w\-]+$"]])
  :post! (fn [{job :edn}]
           (let [datoms (job/edn->datoms job nil)
                 job-id (:db/id (first datoms))]
             ;; 受け付ける形式をEDNだけでなく、Job XMLを受けれるようにする。
             ;; TODO 同一APPで同一ジョブ名は、更新扱いとする。
             (model/transact (conj datoms
                                   [:db/add [:application/name app-name] :application/jobs job-id]))
             job))
  :handle-ok (fn [{{{query :q :keys [limit offset]} :params} :request}]
               (let [jobs (job/find-all app-name query
                                       (to-int offset 0)
                                       (to-int limit 20))]
                 (update-in jobs [:results]
                            #(->> %
                                  (map (fn [{job-name :job/name
                                             executions :job/executions
                                             schedule :job/schedule :as job}]
                                         {:job/name job-name
                                          :job/executions (append-schedule (:db/id job) executions schedule)
                                          :job/latest-execution (find-latest-execution executions)
                                          :job/next-execution   (find-next-execution job)}))
                                  vec)))))

(defn extract-job-parameters [job]
  (->> (edn/read-string (:job/edn-notation job))
       (tree-seq coll? seq)
       (filter #(and (vector? %)
                     (keyword? (first %))
                     (= (name (first %)) "properties")
                     (map? (second %))))
       (map #(->> (second %)
                  (vals)
                  (map (fn [v] (->> (re-seq #"#\{([^\}]+)\}" v)
                                    (map second))))))
       (flatten)
       (map #(->> (re-seq #"jobParameters\['(\w+)'\]" %)
                  (map second)))
       (flatten)
       (apply hash-set)))

(defresource job-resource [app-name job-name]
  :available-media-types ["application/edn"]
  :allowed-methods [:get :put :delete]
  :malformed? #(parse-edn %)
  :exists? (when-let [[app-id job-id] (job/find-by-name app-name job-name)]
             {:app-id app-id
              :job-id job-id})
  :put! (fn [{job :edn job-id :job-id}]
          (model/transact (job/edn->datoms job job-id)))
  :delete! (fn [{job-id :job-id app-id :app-id}]
             (scheduler/unschedule job-id)
             (model/transact [[:db.fn/retractEntity job-id]
                              [:db/retract app-id :application/jobs job-id]]))
  :handle-ok (fn [ctx]
               (let [job (model/pull '[:*
                                       {:job/executions
                                        [:job-execution/start-time
                                         :job-execution/end-time
                                         :job-execution/create-time
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
                            :job/next-execution   (find-next-execution job)
                            :job/dynamic-parameters (extract-job-parameters job))
                     (dissoc :job/executions)))))

(defresource job-settings-resource [app-name job-name & [cmd]]
  :available-media-types ["application/edn"]
  :allowed-methods [:get :delete :put]
  :malformed? #(parse-edn %)
  :exists? (when-let [[app-id job-id] (job/find-by-name app-name job-name)]
             {:app-id app-id
              :job-id job-id}) 
  :put! (fn [{settings :edn job-id :job-id}]
          (case cmd
            :exclusive (model/transact [{:db/id job-id :job/exclusive? true}])
            :status-notification (if-let [id (:db/id settings)]
                                   (model/transact [[:db/retract job-id :job/status-notifications id]])
                                   (model/transact [[:db/add job-id :job/status-notifications #db/id[db.part/user -1]]
                                                    {:db/id #db/id[db.part/user -1]
                                                     :status-notification/batch-status (:status-notification/batch-status settings)
                                                     :status-notification/type         (:status-notification/type settings)}]))
            :time-monitor (model/transact [(merge {:db/id #db/id[db.part/user -1]} settings)
                                           {:db/id job-id :job/time-monitor #db/id[db.part/user -1]}])))
  :delete! (fn [{settings :edn job-id :job-id}]
             (case cmd
               :exclusive (model/transact [{:db/id job-id :job/exclusive? false}])
               :time-monitor (when-let [time-monitor-id (some-> (model/pull '[:job/time-monitor] job-id)
                                                                :job/time-monitor
                                                                :db/id)]
                               (model/transact [[:db/retract job-id :job/time-monitor time-monitor-id]
                                                [:db.fn/retractEntity time-monitor-id]]))))
  :handle-ok (fn [ctx]
               (let [settings (model/pull '[:job/exclusive?
                                            {:job/time-monitor
                                             [:time-monitor/duration
                                              {:time-monitor/action [:db/ident]} 
                                              :time-monitor/notification-type]}
                                            {:job/status-notifications
                                             [:db/id
                                              {:status-notification/batch-status [:db/ident]} 
                                              :status-notification/type]}] (:job-id ctx))]
                 settings)))

(defn- execute-job [app-name job-name ctx]  
  (when-let [[app-id job-id] (job/find-by-name app-name job-name)]
    (let [execution-id (d/tempid :db.part/user)
          tempids (-> (model/transact [{:db/id execution-id
                                       :job-execution/batch-status :batch-status/undispatched
                                       :job-execution/create-time (java.util.Date.)
                                       :job-execution/job-parameters (pr-str (or (:edn ctx) {}))}
                                       [:db/add job-id :job/executions execution-id]])
                      :tempids)]
      (when-let [time-monitor (model/pull '[{:job/time-monitor [:time-monitor/duration
                                                                {:time-monitor/action [:db/ident]}]}] job-id)]
        (scheduler/time-keeper app-name job-name (model/resolve-tempid tempids execution-id)
                               (get-in time-monitor [:job/time-monitor :time-monitor/duration])
                               (get-in time-monitor [:job/time-monitor :time-monitor/action :db/ident]))))))



(defresource executions-resource [app-name job-name]
  :available-media-types ["application/edn"]
  :allowed-methods [:get :post]
  :malformed? #(parse-edn %)
  :exists? (when-let [[app-id job-id] (job/find-by-name app-name job-name)]
             {:job (model/pull '[:job/exclusive?
                                 {:job/executions
                                  [:job-execution/create-time
                                   {:job-execution/batch-status [:db/ident]}]}] job-id)})
  :post-to-existing? (fn [ctx]
                       (when (#{:put :post} (get-in ctx [:request :request-method]))
                         (not (:job/exclusive? (:job ctx)))))  
  :put-to-existing? (fn [ctx]
                      (#{:put :post} (get-in ctx [:request :request-method])))
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
  :put!  #(execute-job app-name job-name %)
  :post! #(execute-job app-name job-name %)
  :handle-ok (fn [{{{:keys [offset limit]} :params} :request}]
               (job/find-executions app-name job-name
                                    (to-int offset 0)
                                    (to-int limit 20))))

(defresource execution-resource [id & [cmd]]
  :available-media-types ["application/edn"]
  :allowed-methods [:get :put]
  :put! (fn [ctx]
          (case cmd
            :abandon (let [execution (model/pull '[:job-execution/execution-id] id)] 
                       (ag/stop-execution (:job-execution/execution-id execution)
                                          :on-success #(ag/update-execution id)))

            :stop (let [execution (model/pull '[:job-execution/execution-id] id)] 
                       (ag/stop-execution (:job-execution/execution-id execution)
                                          :on-success #(ag/update-execution id)))
            
            :alert (let [job (model/query '{:find [(pull ?job [:job/name
                                                               {:job/time-monitor
                                                                [:time-monitor/notification-type]}]) .]
                                            :in [$ ?id]
                                            :where [[?job :job/executions ?id]]} id)]
                     (notification/send (get-in job [:job/time-monitor :time-monitor/notification-type])
                                        {:job/name (:job/name job)}))
            nil))
  :handle-ok (fn [ctx]
               (job/find-execution id)))

(defresource schedule-resource [job-id & [cmd]]
  :available-media-types ["application/edn"]
  :allowed-methods [:post :put :delete]
  :malformed? #(parse-edn %)
  :exists? (fn [ctx]
             (:job/schedule (model/pull '[:job/schedule] job-id)))
  :post! (fn [{schedule :edn}]
           (scheduler/schedule job-id
                               (:schedule/cron-notation schedule)
                               (get-in schedule [:schedule/calendar :calendar/name])))
  :put! (fn [ctx]
          (case cmd
            :pause  (scheduler/pause job-id)
            :resume (scheduler/resume job-id)))
  :delete! (fn [ctx]
             (scheduler/unschedule job-id))
  :handle-ok (fn [ctx])
  :handle-exception (fn [{ex :exception}]
                      (ring-response {:status 500
                                      :body (pr-str {:message (.getMessage ex)})})))

(defresource calendars-resource
  :available-media-types ["application/edn"]
  :allowed-methods [:get :post]
  :malformed? #(validate (parse-edn %)
                         :calendar/name v/required)
  :post! (fn [{cal :edn}]
           (let [id (or (:db/id cal) (d/tempid :db.part/user))]
             (scheduler/add-calendar (:calendar/name cal)
                                     (:calendar/holidays cal)
                                     (:clendar/weekly-holiday cal))
             (model/transact [{:db/id id
                               :calendar/name (:calendar/name cal)
                               :calendar/holidays (:calendar/holidays cal)
                               :calendar/weekly-holiday (pr-str (:calendar/weekly-holiday cal))}])))
  :handle-ok (fn [_]
               (->> (model/query '{:find [[(pull ?cal [:*]) ...]]
                                   :in [$]
                                   :where [[?cal :calendar/name]]})
                    (map (fn [cal]
                           (update-in cal [:calendar/weekly-holiday]
                                      edn/read-string))))))

(defresource calendar-resource [name]
  :available-media-types ["application/edn"]
  :allowed-methods [:get :put :delete]
  :put! (fn [{cal :edn}]
          (model/transact [{:db/id [:calendar/name name]
                            :calendar/name (:calendar/name cal)
                            :calendar/holidays (:calendar/holidays cal)
                            :calendar/weekly-holiday (pr-str (:calendar/weekly-holiday cal))}]))
  :delete! (fn [ctx]
             (model/transact [[:db.fn/retractEntity [:calendar/name name]]]))
  :handle-ok (fn [ctx]
               (-> (model/query '{:find [(pull ?e [:*]) .]
                                   :in [$ ?n]
                                   :where [[?e :calendar/name ?n]]} name)
                   (update-in [:calendar/weekly-holiday] edn/read-string))))

(defresource applications-resource
  :available-media-types ["application/edn"]
  :allowed-methods [:get :post]
  :malformed? #(validate (parse-edn %)
                         :application/name v/required
                         :application/description v/required
                         :application/classpaths v/required) 
  :post! (fn [{app :edn}]
           (if-let [app-id (model/query '[:find ?e . :in $ ?n :where [?e :application/name ?n]] "default")] 
             (model/transact [{:db/id app-id
                               :application/description (:application/description app)
                               :application/classpaths (:application/classpaths app)}])
             (model/transact [{:db/id #db/id[db.part/user -1]
                             :application/name "default" ;; Todo multi applications.
                             :application/description (:application/description app)
                             :application/classpaths (:application/classpaths app)}]))
           (apps/register (assoc app :application/name "default"))
           (when-let [components (apps/scan-components (:application/classpaths app))]
             (let [batch-component-id (model/query '{:find [?c .]
                                                     :in [$ ?app-name]
                                                     :where [[?c :batch-component/application ?app]
                                                             [?app :application/name ?app-name]]} "default")]
               (model/transact [(merge {:db/id (or batch-component-id (d/tempid :db.part/user))
                                        :batch-component/application [:application/name "default"]}
                                       components)]))))
  :handle-ok (fn [ctx]
               (vals @apps/applications)))

(defresource batch-components-resource [app-name]
  :available-media-types ["application/edn"]
  :allowed-methods [:get]
  :handle-ok (fn [ctx]
               (->> (model/query '{:find [?c .]
                                 :in [$ ?app-name]
                                 :where [[?c :batch-component/application ?app]
                                         [?app :application/name ?app-name]]} app-name)
                    (model/pull '[:*]))))
