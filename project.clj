(defproject net.unit8.jobstreamer/job-streamer-control-bus (clojure.string/trim-newline (slurp "VERSION"))
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.stuartsierra/component "0.3.1"]
                 [duct "0.7.0"]
                 [meta-merge "1.0.0"]

                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/core.async "0.2.374"]
                 [javax/javaee-api "7.0"]
                 [environ "1.0.3"]
                 [bouncer "1.0.0" :exclusions [og.clojure/clojurescript]]
                 [net.unit8.wscl/websocket-classloader "0.2.1"]
                 [net.unit8.logback/logback-websocket-appender "0.1.0"]
                 [io.undertow/undertow-websockets-jsr "1.3.22.Final"]
                 [com.datomic/datomic-free "0.9.5372" :exclusions [org.slf4j/slf4j-api org.slf4j/slf4j-nop
                                                                   com.amazonaws/aws-java-sdk]]
                 [org.jsoup/jsoup "1.9.2"]
                 [datomic-schema "1.3.0"]
                 [liberator "0.14.1"]
                 [compojure "1.5.1"]
                 [ring/ring-defaults "0.2.1" :exclusions [javax.servlet/servlet-api]]
                 [ring "1.5.0" :exclusions [ring/ring-jetty-adapter]]
                 [http-kit "2.1.19"]
                 [ch.qos.logback/logback-classic "1.1.7"]
                 [org.jboss.weld.se/weld-se "2.2.7.Final"]
                 [net.unit8.weld/weld-prescan "0.1.0"]

                 ;; for Scheduler
                 [org.quartz-scheduler/quartz "2.2.3"]

                 ;; for monitoring agents
                 [org.rrd4j/rrd4j "2.2.1"]]

  :source-paths ["src/clj"]
  :test-paths   ["test/clj"]
  :java-source-paths ["src/java"]
  :javac-options ["-target" "1.7" "-source" "1.7" "-Xlint:-options"]
  :prep-tasks [["javac"] ["compile"]]

  :plugins [[lein-ring "0.9.7"]
            [lein-libdir "0.1.1"]]
  :libdir-path "lib"
  :main ^:skip-aot job-streamer.control-bus.main

  :pom-plugins [[org.apache.maven.plugins/maven-assembly-plugin "2.5.5"
                 {:configuration [:descriptors [:descriptor "src/assembly/dist.xml"]]}]
                [org.apache.maven.plugins/maven-compiler-plugin "3.3"
                 {:configuration ([:source "1.7"] [:target "1.7"]) }]]

  :profiles
  {:dev  [:project/dev  :profiles/dev]
   :test [:project/test :profiles/test]
   :uberjar {:aot :all}
   :profiles/dev  {}
   :profiles/test {}
   :project/dev   {:dependencies [[duct/generate "0.7.0"]
                                  [reloaded.repl "0.2.2"]
                                  [org.clojure/tools.namespace "0.2.11"]
                                  [org.clojure/tools.nrepl "0.2.12"]
                                  [com.gearswithingears/shrubbery "0.3.1"]
                                  [eftest "0.1.1"]
                                  [kerodon "0.8.0"]]
                   :source-paths ["dev"]
                   :repl-options {:init-ns user}
                   :env {:port "45102"}}
   :project/test {:dependencies [[junit "4.12"]
                                [org.mockito/mockito-all "1.10.19"]
                                [org.powermock/powermock-api-mockito "1.5.4"]
                                [org.powermock/powermock-module-junit4 "1.5.4"]]}})
