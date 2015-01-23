(ns job-streamer.control-bus.scheduler
  (:import [net.unit8.job_streamer.control_bus JobStreamerExecutionJob]
           (org.quartz TriggerBuilder JobBuilder CronScheduleBuilder
                       TriggerKey TriggerUtils CronExpresssion)
           [org.quartz.impl StdSchedulerFactory]))

(defonce scheduler (atom nil))

(defn start [url]
  (reset! scheduler (.getScheduler (StdSchedulerFactory.)))
  (.start @scheduler))

(defn schedule [job-id cron-notation]
  (let [trigger (.. (TriggerBuilder/newTrigger)
                    (withIdentity (str "trigger-" job-id))
                    (withSchedule (CronScheduleBuilder/cronSchedule cron-notation)))
        job-detail (.. (JobBuilder/newJob)
                       (ofType JobStreamerExecutionJob)
                       (withIdentity job-id)
                       (usingJobData "job-id" job-id))])
  (.scheduleJob @scheduler))

(defn fire-times [job-id]
  (let [trigger (.getTrigger @scheduler (TriggerKey. (str "trigger-" job-id)))]
    (TriggerUtils/computeFireTimes trigger nil 5)))

(defn validate-format [cron-notation]
  (CronExpresssion/validateExpression cron-notation))

(defn stop []
  (.shutdown @scheduler)
  (reset! scheduler nil))


