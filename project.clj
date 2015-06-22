(defproject net.unit8.jobstreamer/job-streamer-control-bus "0.2.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.7.0-RC1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [javax/javaee-api "7.0"]
                 [environ "1.0.0"]
                 [bouncer "0.3.2"]
                 [net.unit8.wscl/websocket-classloader "0.2.1"]
                 [net.unit8.logback/logback-websocket-appender "0.1.0"]
                 [io.undertow/undertow-websockets-jsr "1.1.1.Final"]
                 [com.datomic/datomic-free "0.9.5130" :exclusions [org.slf4j/slf4j-api org.slf4j/slf4j-nop] ]
                 [datomic-schema "1.3.0"]
                 [liberator "0.12.2"]
                 [compojure "1.3.2"]
                 [ring/ring-defaults "0.1.4"]
                 [ring "1.3.2"]
                 [http-kit "2.1.19"]
                 [ch.qos.logback/logback-classic "1.1.3"]
                 [org.jboss.weld.se/weld-se "2.2.7.Final"]
                 [net.unit8.weld/weld-prescan "0.1.0-SNAPSHOT"]

                 ;; for Scheduler
                 [org.quartz-scheduler/quartz "2.2.1"]

                 ;; for monitoring agents
                 [org.rrd4j/rrd4j "2.2"]]

  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]

  :plugins [[lein-ring "0.8.13"]
            [lein-libdir "0.1.1"]]
  :libdir-path "lib"

  :main job-streamer.control-bus.core
  :ring {:handler job-streamer.control-bus.core/app
         :init    job-streamer.control-bus.core/init}
  :profiles {:test {:dependencies [[junit "4.125"]]}})
