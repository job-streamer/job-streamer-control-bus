(ns job-streamer.control-bus.component.scheduler
  (:require [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [liberator.core :as liberator]
            [liberator.representation :refer [ring-response]]
            [clojure.string :as str]
            [bouncer.core :as b]
            [bouncer.validators :as v]
            (job-streamer.control-bus [util :refer [parse-body]])
            (job-streamer.control-bus.component [datomic :as d]))
  (:import [net.unit8.job_streamer.control_bus JobStreamerExecuteJob TimeKeeperJob HolidayAndWeeklyCalendar]
           (org.quartz TriggerBuilder JobBuilder CronScheduleBuilder DateBuilder DateBuilder$IntervalUnit
                       TriggerKey TriggerUtils CronExpression
                       Trigger$TriggerState)
           [org.quartz.impl StdSchedulerFactory]))

(defn- make-trigger [job-id cron-notation calendar-name]
  (let [builder (.. (TriggerBuilder/newTrigger)
                    (withIdentity (str "trigger-" job-id))
                    (withSchedule (CronScheduleBuilder/cronSchedule cron-notation)))]
    (when calendar-name
      (.modifiedByCalendar builder calendar-name))
    (.build builder)))

(defn time-keeper [{:keys [scheduler datomic host port]}
                   execution-id duration action]
  (let [[app-name job-name] (d/query datomic
                                     '{:find [[?app-name ?job-name]]
                                       :in [$ ?execution-id]
                                       :where [[?app :application/name ?app-name]
                                               [?app :application/jobs ?job]
                                               [?job :job/name ?job-name]
                                               [?job :job/executions ?execution-id]]} execution-id)
        trigger (.. (TriggerBuilder/newTrigger)
                    (startAt (DateBuilder/futureDate duration DateBuilder$IntervalUnit/MINUTE))
                    (build))
        job-deail (.. (JobBuilder/newJob)
                      (ofType TimeKeeperJob)
                      (withIdentity (str "time-keeper-" execution-id))
                      (usingJobData "app-name" app-name)
                      (usingJobData "job-name" job-name)
                      (usingJobData "execution-id" execution-id)
                      (usingJobData "command" (name action))
                      (usingJobData "host" host)
                      (usingJobData "port" (long port))
                      (build))]
    (.scheduleJob scheduler job-deail trigger)))

(defn schedule [{:keys [datomic scheduler host port]} job-id cron-notation calendar-name]
  (let [new-trigger (make-trigger job-id cron-notation calendar-name)
        job (d/pull datomic
                    '[:job/name
                      {:job/schedule
                       [:db/id
                        :schedule/cron-notation]}] job-id)
        app-name (d/query datomic
                          '{:find [?app-name .]
                            :in [$ ?job-id]
                            :where [[?app :application/name ?app-name]
                                    [?app :application/jobs ?job-id]]} job-id)
        job-detail (.. (JobBuilder/newJob)
                       (ofType JobStreamerExecuteJob)
                       (withIdentity (str "job-" job-id))
                       (usingJobData "app-name" app-name)
                       (usingJobData "job-name" (:job/name job))
                       (usingJobData "host" host)
                       (usingJobData "port" (long port))
                       (build))]
    (if-let [trigger (.getTrigger scheduler (TriggerKey. (str "trigger-" job-id)))]
      (do
        (.rescheduleJob scheduler (.getKey trigger) new-trigger)
        (d/transact datomic
                    [(merge {:db/id (get-in job [:job/schedule :db/id])
                             :schedule/cron-notation cron-notation
                             :schedule/active? true}
                            (when calendar-name
                              {:schedule/calendar [:calendar/name calendar-name]})) ]))
      (do
        (.scheduleJob scheduler job-detail new-trigger)
        (d/transact datomic
                    [(merge {:db/id #db/id[db.part/user -1]
                             :schedule/cron-notation cron-notation
                             :schedule/active? true}
                            (when calendar-name
                              {:schedule/calendar [:calendar/name calendar-name]}))
                         {:db/id job-id
                          :job/schedule #db/id[db.part/user -1]}])))))

(defn pause [{:keys [datomic scheduler]} job-id]
  (let [job (d/pull datomic
                    '[:job/id
                      {:job/schedule
                       [:db/id]}] job-id)]
    (.pauseTrigger scheduler (TriggerKey. (str "trigger-" job-id)))
    (d/transact datomic
                [{:db/id (get-in job [:job/schedule :db/id])
                  :schedule/active? false}])))

(defn resume [{:keys [datomic scheduler]} job-id]
  (let [job (d/pull datomic
                    '[:job/id
                      {:job/schedule
                       [:db/id]}] job-id)]
    (.resumeTrigger scheduler (TriggerKey. (str "trigger-" job-id)))
    (d/transact datomic
                [{:db/id (get-in job [:job/schedule :db/id])
                  :schedule/active? true}])))

(defn unschedule [{:keys [scheduler datomic]} job-id]
  (let [job (d/pull datomic
                    '[{:job/schedule
                       [:db/id]}] job-id)]
    (.unscheduleJob scheduler (TriggerKey. (str "trigger-" job-id)))
    (when-let [schedule (get-in job [:job/schedule :db/id])]
      (d/transact datomic [[:db.fn/retractEntity schedule]]))))

(defn fire-times [{:keys [scheduler]} job-id]
  (let [trigger-key (TriggerKey. (str "trigger-" job-id))
        trigger-state (.getTriggerState scheduler trigger-key)
        trigger (.getTrigger scheduler trigger-key)]
    (when (= trigger-state Trigger$TriggerState/NORMAL)
      (TriggerUtils/computeFireTimes trigger nil 5))))

(defn validate-format [cron-notation]
  (CronExpression/validateExpression cron-notation))

(defn hh:mm? [hh:mm-string]
  (if hh:mm-string
    (re-find #"^([01]?[0-9]|2[0-3]):([0-5][0-9])$" hh:mm-string)
    false))

(defn to-ms-from-hh:mm [hh:mm-string]
  (if-not (hh:mm? hh:mm-string)
    0
     (let [[hh mm] (-> hh:mm-string (str/split #":") (#(map (fn [s] (Integer/parseInt s)) %)))]
      (* (+ (* hh 60) mm) 60000))))

(defn add-calendar [{:keys [scheduler]} calendar]
  (let [holiday-calendar (HolidayAndWeeklyCalendar.)]
    (doseq [holiday (:calendar/holidays calendar)]
      (.addExcludedDate holiday-calendar holiday))
    (.setDaysExcluded holiday-calendar (boolean-array (:calendar/weekly-holiday calendar)))
    (.setDayStart holiday-calendar (to-ms-from-hh:mm (:calendar/day-start calendar)))
    (.addCalendar scheduler (:calendar/name calendar) holiday-calendar false false)))

(defn delete-calendar[{:keys [scheduler]} calendar-name]
  (.deleteCalendar scheduler calendar-name))

(defn entry-resource [{:keys [datomic] :as scheduler} job-id & [cmd]]
  (liberator/resource
   :available-media-types ["application/edn" "application/json"]
   :allowed-methods [:post :put :delete]
   :malformed? #(parse-body %)
   :exists? (fn [ctx]
              (:job/schedule (d/pull datomic '[:job/schedule] job-id)))
   :allowed? (fn [{{:keys [request-method identity]} :request}]
               (let [permissions (:permissions identity)]
                 (condp = request-method
                   :post (:permission/execute-job permissions)
                   :put (:permission/execute-job permissions)
                   :delete (:permission/execute-job permissions)
                   false)))
   :post! (fn [{s :edn}]
            (schedule scheduler job-id
                      (:schedule/cron-notation s)
                      (get-in s [:schedule/calendar :calendar/name])))
   :put! (fn [ctx]
           (case cmd
             :pause  (pause  scheduler job-id)
             :resume (resume scheduler job-id)))
   :delete! (fn [ctx]
              (unschedule scheduler job-id))
   :handle-ok (fn [ctx])
   :handle-exception (fn [{ex :exception}]
                       (log/error "scheduler resource:" ex)
                       (ring-response
                        {:status 500
                         :body (pr-str {:message (.getMessage ex)})}))))

(defrecord Scheduler [datomic host port]
  component/Lifecycle

  (start [component]
    (let [scheduler (.getScheduler (StdSchedulerFactory.))]
      (doseq [calendar (d/query datomic
                                '{:find [[(pull ?calendar [:*]) ...]]
                                  :where [[?calendar :calendar/name]]})]
        (add-calendar (assoc component :scheduler scheduler)
                      (update-in calendar [:calendar/weekly-holiday] edn/read-string)))
      (.start scheduler)
      (log/info "started scheduler.")
      (let [schedules (d/query datomic
                               '{:find [?job ?schedule]
                                 :where [[?job :job/schedule ?schedule]]})]
        (doseq [[job-id sched] schedules]
          (let [s (d/pull datomic
                          '[:schedule/cron-notation
                            {:schedule/calendar [:calendar/name]}]
                          sched)]
            (log/info "Recover schedule: " job-id)
            (schedule (assoc component :scheduler scheduler)
                      job-id
                      (:schedule/cron-notation s)
                      (get-in s [:schedule/calendar :calendar/name])))))
      (assoc component :scheduler scheduler)))

  (stop [component]
    (if-let [scheduler (:scheduler component)]
      (.shutdown scheduler)
      (log/info "stop scheduler."))
    (dissoc component :scheduler)))

(defn scheduler-component [options]
  (map->Scheduler options))
