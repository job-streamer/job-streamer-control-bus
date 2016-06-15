(ns job-streamer.control-bus.main
  (:require [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [compojure.route :as route]
            [compojure.core :refer [defroutes GET ANY]]
            [ring.middleware.defaults :refer [wrap-defaults  api-defaults]]
            [ring.middleware.reload :refer [wrap-reload]]
            (job-streamer.control-bus (model :as model)
                                      (job :as job)
                                      (apps :as apps)
                                      (agent :as ag)
                                      (broadcast :as broadcast)
                                      (server :as server)
                                      (scheduler :as scheduler)
                                      (dispatcher :as dispatcher)
                                      (recovery :as recovery)))
  (:use [clojure.core.async :only [chan put! <! go go-loop timeout]]
        [environ.core :only [env]]
        [liberator.representation :only [ring-response]]
        [job-streamer.control-bus.api]
        [ring.util.response :only [header]])
  (:gen-class))

(def banner "
    __     _   _____ _                 Control Bus
 __|  |___| |_|   __| |_ ___ ___ ___ _____ ___ ___ 
|  |  | . | . |__   |  _|  _| -_| .'|     | -_|  _|
|_____|___|___|_____|_| |_| |___|__,|_|_|_|___|_|
")

(defn -main [& args]
  (let [port (Integer/parseInt (or (:control_bus-port env) "45102"))]
    (go-loop []
      (<! (timeout 10000))
      (try
        (recovery/update-job-status)
        (catch Throwable t
          (log/error t)))
      (recur))
    (println banner)))
