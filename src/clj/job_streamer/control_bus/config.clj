(ns job-streamer.control-bus.config
  (:require [environ.core :refer [env]]
            [job-streamer.control-bus.model :as model]))

(def defaults
  {:http {:port 45102}
   :discoverer {:ws-port 45102}
   :migration {:dbschema model/dbschema}})

(def environ
  {:http {:port (some-> env :control_bus-port Integer.)}
   :discoverer {:ws-port (some-> env :control_bus-port Integer.)}})

