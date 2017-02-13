(ns job-streamer.control-bus.main
  (:gen-class)
  (:require [com.stuartsierra.component :as component]
            [duct.middleware.errors :refer [wrap-hide-errors]]
            [duct.util.runtime :refer [add-shutdown-hook]]
            [meta-merge.core :refer [meta-merge]]
            (job-streamer.control-bus [config :as config]
                                      [system :refer [new-system]])))

(def prod-config
  {:app {:middleware     [[wrap-hide-errors :internal-error]]
         :internal-error "Internal Server Error"}})

(def config
  (meta-merge config/defaults
              config/environ
              prod-config))

(def banner "
    __     _   _____ _                 Control Bus
 __|  |___| |_|   __| |_ ___ ___ ___ _____ ___ ___
|  |  | . | . |__   |  _|  _| -_| .'|     | -_|  _|
|_____|___|___|_____|_| |_| |___|__,|_|_|_|___|_|
")

(def system
  (atom nil))

(defn -main [& args]
    (reset! system (new-system config))
    (println banner "Starting HTTP server on port" (-> @system :http :port))
    (add-shutdown-hook ::stop-system #(component/stop @system))
    (swap! system component/start))

