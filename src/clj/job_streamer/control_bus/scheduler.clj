(ns job-streamer.control-bus.scheduler
  (:require [job-streamer.control-bus.model :as model]
            [clojure.tools.logging :as log])
  (:import [net.unit8.job_streamer.control_bus JobStreamerExecuteJob TimeKeeperJob]
           (org.quartz TriggerBuilder JobBuilder CronScheduleBuilder DateBuilder DateBuilder$IntervalUnit
                       TriggerKey TriggerUtils CronExpression
                       Trigger$TriggerState)
           [org.quartz.impl StdSchedulerFactory]))

(defonce scheduler (atom nil))
(defonce control-bus (atom {}))

(defn- make-trigger [job-id cron-notation]
  (.. (TriggerBuilder/newTrigger)
      (withIdentity (str "trigger-" job-id))
      (withSchedule (CronScheduleBuilder/cronSchedule cron-notation))
      (build)))

(defn time-keeper [app-name job-name execution-id duration action]
  (let [trigger (.. (TriggerBuilder/newTrigger)
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

(defn schedule [job-id cron-notation]
  (let [new-trigger (make-trigger job-id cron-notation)
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
        (model/transact [{:db/id (get-in job [:job/schedule :db/id])
                          :schedule/cron-notation cron-notation
                          :schedule/active? true}]))
      (do
        (.scheduleJob @scheduler job-detail new-trigger)
        (model/transact [{:db/id #db/id[db.part/user -1]
                          :schedule/cron-notation cron-notation
                          :schedule/active? true}
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

(defn start [host port]
  (swap! control-bus assoc :host host :port (int port))
  (reset! scheduler (.getScheduler (StdSchedulerFactory.)))
  (.start @scheduler)
  (log/info "started scheduler.")
  (let [schedules (model/query '{:find [?job ?cron-notation]
                                 :where [[?job :job/schedule ?schedule]
                                         [?schedule :schedule/cron-notation ?cron-notation]]})]
    (doseq [[job-id cron-notation] schedules]
      (log/info "Recover schedule: " job-id cron-notation)
      (schedule job-id cron-notation))))

(defn stop []
  (.shutdown @scheduler)
  (log/info "stop scheduler.")
  (reset! scheduler nil))


