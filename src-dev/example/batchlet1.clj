(ns example.batchlet1
  (:gen-class
   :name example.Batchlet1
   :implements [javax.batch.api.Batchlet]
   :prefix "-")
  (:import [org.slf4j LoggerFactory]))

(def logger (LoggerFactory/getLogger "job-streamer"))

(defn -process [this]
  (.info logger "start the job!")
  (.warn logger "warning!")
  (.error logger "error!" (IllegalArgumentException.))
  (println "================================")
  (println "    Hello JobStreamer!!!")
  (println "================================")
  (.info logger "Ended the job!")
  "SUCCESS")

(defn -stop [this])
