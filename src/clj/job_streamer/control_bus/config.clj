(ns job-streamer.control-bus.config
  (:require [environ.core :refer [env]]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [job-streamer.control-bus.model :as model]))

(def defaults
  {:http {:port 45102}
   :app {:same-origin {:access-control-allow-origin "http://localhost:3000"}
         :session-timeout {:timeout (* 30 60)}}
   :discoverer {:ws-port 45102}
   :scheduler  {:host "localhost"
                :port 45102}
   :migration {:dbschemas model/dbschemas}
   :token {:session-timeout (* 30 60)}
   :auth {:access-control-allow-origin "http://localhost:3000"}
   :datomic {:uri "datomic:mem://job-streamer"}})

(def environ
  (let [port (some-> env :control-bus-port Integer.)
        datomic-uri (:datomic-uri env)
        access-control-allow-origin (some-> env :access-control-allow-origin)
        session-timeout (some-> env :session-timeout Integer. (* 60))]
  {:http {:port port}
   :app {:same-origin {:access-control-allow-origin access-control-allow-origin}
         :session-timeout {:timeout session-timeout}}
   :discoverer {:ws-port port}
   :scheduler  {:host "localhost"
                :port port}
   :token {:session-timeout session-timeout}
   :auth {:access-control-allow-origin access-control-allow-origin}
   :datomic {:uri datomic-uri}}))

(def resource-file
  (some-> "job-streamer-control-bus/config.edn"
          io/resource
          slurp
          edn/read-string))
