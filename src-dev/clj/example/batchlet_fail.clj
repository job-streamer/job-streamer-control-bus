(ns example.batchlet-fail
  (:gen-class
   :name example.FailBatchlet
   :implements [javax.batch.api.Batchlet]
   :prefix "-")
  (:import [org.slf4j LoggerFactory]))

(def logger (LoggerFactory/getLogger "job-streamer"))

(defn -process [this]
  (.info logger "start the job!")
  (println "================================")
  (println "    Fail Batchlet")
  (println "================================")
  (throw (Exception. "Something is wrong."))
  (.info logger "Ended the job!")
  "SUCCESS")

(defn -stop [this])
