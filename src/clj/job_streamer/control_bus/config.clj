(ns job-streamer.control-bus.config
  (:require [environ.core :refer [env]]
            [job-streamer.control-bus.model :as model]))

(def defaults
  {:http {:port 45102}
   :discoverer {:ws-port 45102}
   :scheduler  {:host "localhost"
                :port 45102}
   :migration {:dbschema model/dbschema}})

(def environ
  (let [port (some-> env :control-bus-port Integer.)]
  {:http {:port port}
   :discoverer {:ws-port port}
   :scheduler  {:host "localhost"
                :port port}}))

