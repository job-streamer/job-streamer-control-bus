(ns job-streamer.control-bus.scheduler
  (:require [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [job-streamer.control-bus.model :as model])
  (:import [net.unit8.job_streamer.control_bus JobStreamerExecuteJob TimeKeeperJob HolidayAndWeeklyCalendar]
           (org.quartz TriggerBuilder JobBuilder CronScheduleBuilder DateBuilder DateBuilder$IntervalUnit
                       TriggerKey TriggerUtils CronExpression
                       Trigger$TriggerState)
           [org.quartz.impl StdSchedulerFactory]))

(defonce scheduler (atom nil))
(defonce control-bus (atom {}))

(defn- make-trigger [job-id cron-notation calendar-name]
  (let [builder (.. (TriggerBuilder/newTrigger)
                    (withIdentity (str "trigger-" job-id))
                    (withSchedule (CronScheduleBuilder/cronSchedule cron-notation)))]
    (when calendar-name
      (.modifiedByCalendar builder calendar-name))
    (.build builder)))

(defn time-keeper [execution-id duration action]
  (let [[app-name job-name] (model/query '{:find [[?app-name ?job-name]]
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
                      (usingJobData "host" (:host @control-bus))
                      (usingJobData "port" (:port @control-bus))
                      (build))]
    (.scheduleJob @scheduler job-deail trigger)))

(defn schedule [job-id cron-notation calendar-name]
  (let [new-trigger (make-trigger job-id cron-notation calendar-name)
        job (model/pull '[:job/name
                          {:job/schedule
                           [:db/id
                            :schedule/cron-notation]}] job-id)
        app-name (model/query '{:find [?app-name .]
                                :in [$ ?job-id]
                                :where [[?app :application/name ?app-name]
                                        [?app :application/jobs ?job-id]]} job-id)
        job-detail (.. (JobBuilder/newJob)
                       (ofType JobStreamerExecuteJob)
                       (withIdentity (str "job-" job-id))
                       (usingJobData "app-name" app-name)
                       (usingJobData "job-name" (:job/name job))
                       (usingJobData "host" (:host @control-bus))
                       (usingJobData "port" (:port @control-bus))
                       (build))]
    (if-let [trigger (.getTrigger @scheduler (TriggerKey. (str "trigger-" job-id)))]
      (do
        (.rescheduleJob @scheduler (.getKey trigger) new-trigger)
        (model/transact [(merge {:db/id (get-in job [:job/schedule :db/id])
                                 :schedule/cron-notation cron-notation
                                 :schedule/active? true}
                                (when calendar-name {:schedule/calendar [:calendar/name calendar-name]})) ]))
      (do
        (.scheduleJob @scheduler job-detail new-trigger)
        (model/transact [(merge {:db/id #db/id[db.part/user -1]
                                 :schedule/cron-notation cron-notation
                                 :schedule/active? true}
                                (when calendar-name {:schedule/calendar [:calendar/name calendar-name]}))
                         {:db/id job-id
                          :job/schedule #db/id[db.part/user -1]}])))))

(defn pause [job-id]
  (let [job (model/pull '[:job/id
                          {:job/schedule
                           [:db/id]}] job-id)]
    (.pauseTrigger @scheduler (TriggerKey. (str "trigger-" job-id)))
    (model/transact [{:db/id (get-in job [:job/schedule :db/id])
                      :schedule/active? false}])))

(defn resume [job-id]
  (let [job (model/pull '[:job/id
                          {:job/schedule
                           [:db/id]}] job-id)]
    (.resumeTrigger @scheduler (TriggerKey. (str "trigger-" job-id)))
    (model/transact [{:db/id (get-in job [:job/schedule :db/id])
                      :schedule/active? true}])))

(defn unschedule [job-id]
  (let [job (model/pull '[{:job/schedule
                           [:db/id]}] job-id)]
    (.unscheduleJob @scheduler (TriggerKey. (str "trigger-" job-id)))
    (when-let [schedule (get-in job [:job/schedule :db/id])]
      (model/transact [[:db.fn/retractEntity schedule]]))))

(defn fire-times [job-id]
  (let [trigger-key (TriggerKey. (str "trigger-" job-id))
        trigger-state (.getTriggerState @scheduler trigger-key)
        trigger (.getTrigger @scheduler trigger-key)]
    (when (= trigger-state Trigger$TriggerState/NORMAL)
      (TriggerUtils/computeFireTimes trigger nil 5))))

(defn validate-format [cron-notation]
  (CronExpression/validateExpression cron-notation))

(defn add-calendar [calendar]
  (let [holiday-calendar (HolidayAndWeeklyCalendar.)]
    (doseq [holiday (:calendar/holidays calendar)]
      (.addExcludedDate holiday-calendar holiday))
    (.setDaysExcluded holiday-calendar (boolean-array (:calendar/weekly-holiday calendar)))
    (.addCalendar @scheduler (:calendar/name calendar) holiday-calendar false false)))

(defn start [host port]
  (swap! control-bus assoc :host host :port (int port))
  (reset! scheduler (.getScheduler (StdSchedulerFactory.)))

  (doseq [calendar (model/query '{:find [[(pull ?calendar [:*]) ...]]
                                   :where [[?calendar :calendar/name]]})]
    (add-calendar (update-in calendar [:calendar/weekly-holiday] edn/read-string)))

  (.start @scheduler)
  (log/info "started scheduler.")
  (let [schedules (model/query '{:find [?job ?schedule]
                                 :where [[?job :job/schedule ?schedule]]})]
    (doseq [[job-id sched] schedules]
      (let [s (model/pull '[:schedule/cron-notation {:schedule/calendar [:calendar/name]}] sched)]
        (log/info "Recover schedule: " job-id)
        (schedule job-id
                  (:schedule/cron-notation s)
                  (get-in s [:schedule/calendar :calendar/name]))))))

(defn stop []
  (.shutdown @scheduler)
  (log/info "stop scheduler.")
  (reset! scheduler nil))


