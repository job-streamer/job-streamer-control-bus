(ns job-streamer.control-bus.rrd
  (:refer-clojure :exclude [update])
  (:import [java.nio.file Files Paths LinkOption]
           [java.nio.file.attribute FileAttribute]
           [java.awt Color]
           [org.rrd4j ConsolFun DsType]
           [org.rrd4j.core RrdDef RrdDbPool Util]
           [org.rrd4j.graph RrdGraph RrdGraphDef]))

(def db-pool (RrdDbPool/getInstance))

(defn create-rrd-def [rrd-path]
  (let [rrd-def (RrdDef. (.toString rrd-path) (long 300))]
    (doto rrd-def
      (.addArchive ConsolFun/AVERAGE 0.5 1  288)   ; 1 day   / 5min
      (.addArchive ConsolFun/AVERAGE 0.5 3  672)   ; 1 week  / 15min
      (.addArchive ConsolFun/AVERAGE 0.5 12 744)   ; 1 month / 1hour
      (.addArchive ConsolFun/AVERAGE 0.5 72 1480)  ; 1 year  / 6hour
      (.addDatasource "load-process"    DsType/GAUGE 300, 0, Double/NaN)
      (.addDatasource "load-system"     DsType/GAUGE 300, 0, Double/NaN)
      (.addDatasource "memory-physical" DsType/GAUGE 300, 0, Double/NaN)
      (.addDatasource "memory-swap"     DsType/GAUGE 300, 0, Double/NaN))
    rrd-def))

(defn- rrd-db [rrd-path]
  (. db-pool requestRrdDb
     (if (Files/exists rrd-path (into-array LinkOption []))
       (.toString rrd-path)
       (create-rrd-def rrd-path))))

(defn create-rrd-path [agt]
  (let [rrd-file (str (:agent/instance-id agt) ".rrd")
        path (Paths/get "rrd" (into-array String [rrd-file]))]
    (when-not (Files/exists (.getParent path) (into-array LinkOption []))
      (Files/createDirectory (.getParent path) (into-array FileAttribute [])))
    path))

(defn update [agt]
  (let [db (rrd-db (create-rrd-path agt))]
    (try
      (doto (.createSample db (Util/getTimestamp))
        (.setValue "load-process"    (double (get-in agt [:agent/stats :cpu :process :load] 0)))
        (.setValue "load-system"     (double (get-in agt [:agent/stats :cpu :system  :load] 0)))
        (.setValue "memory-physical" (double (get-in agt [:agent/stats :memory :physical :free] 0)))
        (.setValue "memory-swap"     (double (get-in agt [:agent/stats :memory :swap     :free] 0)))
        (.update))
      (finally (.release db-pool db)))))

(defn render-graph [agt type]
  (let [g-def (RrdGraphDef.)
        now (Util/getTimestamp)
        rrd-path (create-rrd-path agt)]
    (doto g-def
      (.setStartTime (- now (* 24 60 60)))
      (.setEndTime   now)
      (.setWidth 500)
      (.setHeight 300)
      (.setFilename "-")
      (.setPoolUsed true)
      (.setImageFormat "png"))
    (case type
      "memory" (doto g-def
                 (.setTitle "Memory usage")
                 (.setVerticalLabel "bytes")
                 (.line "Free memory (physical)" Color/GREEN)
                 (.line "Free memory (swap)"  Color/BLUE)
                 (.datasource "Free memory (physical)" (.toString rrd-path) "memory-physical" ConsolFun/AVERAGE)
                 (.datasource "Free memory (swap)"     (.toString rrd-path) "memory-swap" ConsolFun/AVERAGE)
                 (.gprint "Free memory (physical)" ConsolFun/AVERAGE "Free memory (physical) = %.3f%s")
                 (.gprint "Free memory (swap)" ConsolFun/AVERAGE "Free memory (swap) = %.3f%s\\c"))
      "cpu"    (doto g-def
                 (.setTitle "CPU usage")
                 (.line "CPU usage (process)" Color/GREEN)
                 (.line "CPU usage (system)"  Color/BLUE)
                 (.datasource "CPU usage (process)" (.toString rrd-path) "load-process" ConsolFun/AVERAGE)
                 (.datasource "CPU usage (system)"  (.toString rrd-path) "load-system" ConsolFun/AVERAGE)
                 (.gprint "CPU usage (process)" ConsolFun/AVERAGE "CPU usage (process) = %.3f%s")
                 (.gprint "CPU usage (system)" ConsolFun/AVERAGE "CPU usage (system) = %.3f%s\\c")))
    (.. (RrdGraph. g-def) getRrdGraphInfo getBytes)))

