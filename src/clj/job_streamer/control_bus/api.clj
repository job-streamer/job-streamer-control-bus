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
                                      (dispatcher :as dispatcher)))
  (:use [clojure.core.async :only [chan put! <! go-loop timeout]]
        [clojure.walk]
        [liberator.representation :only [ring-response]]
        [ring.util.response :only [header]]
        (job-streamer.control-bus (agent :only [find-agent execute-job available-agents] :as ag)
                                  (validation :only [validate])))
  (:import [java.util Date]))

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
                  :jobs   (model/query '{:find [(count ?job) .]
                                         :in [$ ?app-name]
                                         :where [[?app :application/name ?app-name]
                                                 [?app :application/jobs ?job]]}
                                       app-name)}))

(defresource jobs-resource [app-name]
  :available-media-types ["application/edn"]
  :allowed-methods [:get :post]
  :malformed? #(validate (parse-edn %)
                         :job/name [v/required [v/matches #"^[\w\-]+$"]])
  :post! (fn [{job :edn}]
           (let [datoms (job/edn->datoms job nil)
                 job-id (:db/id (first datoms))]
             (model/transact (conj datoms
                                   [:db/add [:application/name app-name] :application/jobs job-id]))
             job))
  :handle-ok (fn [{{{query "q"} :query-params} :request}]
               (let []
                 (vec (map (fn [{job-name :job/name
                                 executions :job/executions
                                 schedule :job/schedule :as job}]
                             {:job/name job-name
                              :job/executions (append-schedule (:db/id job) executions schedule)
                              :job/latest-execution (find-latest-execution executions)
                              :job/next-execution   (find-next-execution job)})
                           (job/find-all app-name query))))))

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
                                         {:job-execution/agent [:agent/name]}]}
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
                            :job/next-execution   (find-next-execution job))
                     (dissoc :job/executions)))))

(defresource executions-resource [app-name job-name]
  :available-media-types ["application/edn"]
  :allowed-methods [:get :post]
  :malformed? #(parse-edn %)
  :exists? (when-let [[app-id job-id] (job/find-by-name app-name job-name)]
             {:job-id job-id})
  :if-match-star-exists-for-missing? (fn [{job-id :job-id}]
                                       (nil? job-id))
  :post! (fn [ctx]
           (when-let [[app-id job-id] (job/find-by-name app-name job-name)]
             (model/transact [{:db/id #db/id[db.part/user -1]
                               :job-execution/batch-status :batch-status/undispatched
                               :job-execution/create-time (java.util.Date.)
                               :job-execution/job-parameters (pr-str (or (:edn ctx) {}))}
                              [:db/add job-id :job/executions #db/id[db.part/user -1]]])))
  :handle-ok (fn [ctx]
               (job/find-executions app-name job-name)))

(defresource execution-resource [id & [cmd]]
  :available-media-types ["application/edn"]
  :allowed-methods [:get :put]
  :put! (fn [ctx]
          (model/transact [{:db/id id
                            :job-execution/batch-status :batch-status/stopped
                            :job-execution/end-time (java.util.Date.)}]))
  :handle-ok (fn [ctx]
               (job/find-execution id)))

(defresource schedule-resource [job-id & [cmd]]
  :available-media-types ["application/edn"]
  :allowed-methods [:post :put :delete]
  :malformed? #(parse-edn %)
  :exists? (fn [ctx]
             (:job/schedule (model/pull '[:job/schedule] job-id)))
  :post! (fn [ctx]
           (scheduler/schedule job-id (get-in ctx [:edn :schedule/cron-notation])))
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
           (apps/register (assoc app :name "default"))
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
