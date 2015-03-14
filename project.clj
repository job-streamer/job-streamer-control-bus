(defproject net.unit8.jobstreamer/job-streamer-control-bus "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [javax/javaee-api "7.0"]
                 [environ "1.0.0"]
                 [bouncer "0.3.2"]
                 [net.unit8.wscl/websocket-classloader "0.2.1"]
                 [net.unit8.logback/logback-websocket-appender "0.1.0"]
                 [io.undertow/undertow-websockets-jsr "1.1.1.Final"]
                 [com.datomic/datomic-free "0.9.5130" :exclusions [org.slf4j/slf4j-api org.slf4j/slf4j-nop] ]
                 [datomic-schema "1.2.1"]
                 [liberator "0.12.2"]
                 [compojure "1.3.1"]
                 [ring/ring-defaults "0.1.3"]
                 [ring "1.3.2"]
                 [http-kit "2.1.17"]
                 [ch.qos.logback/logback-classic "1.1.2"]

                 ;; for Scheduler
                 [org.quartz-scheduler/quartz "2.2.1"]

                 ;; for monitoring agents
                 [org.rrd4j/rrd4j "2.2"]]

  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]

  :plugins [[lein-ring "0.8.13"]]
  :main job-streamer.control-bus.core
  :ring {:handler job-streamer.control-bus.core/app
         :init    job-streamer.control-bus.core/init})
