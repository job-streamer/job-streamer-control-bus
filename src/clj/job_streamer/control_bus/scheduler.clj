(ns job-streamer.control-bus.scheduler
  (:require [job-streamer.control-bus.model :as model]
            [clojure.tools.logging :as log])
  (:import [net.unit8.job_streamer.control_bus JobStreamerExecuteJob]
           (org.quartz TriggerBuilder JobBuilder CronScheduleBuilder
                       TriggerKey TriggerUtils CronExpression)
           [org.quartz.impl StdSchedulerFactory]))

(defonce scheduler (atom nil))
(defonce control-bus (atom {}))

(defn start [host port]
  (swap! control-bus assoc :host host :port (int port))
  (reset! scheduler (.getScheduler (StdSchedulerFactory.)))
  (.start @scheduler)
  (log/info "started scheduler.")
  (model/query '{:find [?job-id ?cron-notation]
                 :where [[?job :job/schedule ?schedule]
                         [?job :job/id ?job-id]
                         [?schedule :schedule/cron-notation ?cron-notation]]}))

(defn schedule [job-id cron-notation]
  (let [trigger (.. (TriggerBuilder/newTrigger)
                    (withIdentity (str "trigger-" job-id))
                    (withSchedule (CronScheduleBuilder/cronSchedule cron-notation))
                    (build))
        job-detail (.. (JobBuilder/newJob)
                       (ofType JobStreamerExecuteJob)
                       (withIdentity job-id)
                       (usingJobData "job-id" job-id)
                       (usingJobData "host" (:host @control-bus))
                       (usingJobData "port" (:port @control-bus))
                       (build))]
    (.scheduleJob @scheduler job-detail trigger)
    (model/transact [{:db/id #db/id[db.part/user -1]
                    :schedule/cron-notation cron-notation}
                   {:db/id [:job/id job-id]
                    :job/schedule #db/id[db.part/user -1]}])))

(defn fire-times [job-id]
  (let [trigger (.getTrigger @scheduler (TriggerKey. (str "trigger-" job-id)))]
    (TriggerUtils/computeFireTimes trigger nil 5)))

(defn validate-format [cron-notation]
  (CronExpression/validateExpression cron-notation))

(defn stop []
  (.shutdown @scheduler)
  (log/info "stop scheduler.")
  (reset! scheduler nil))


