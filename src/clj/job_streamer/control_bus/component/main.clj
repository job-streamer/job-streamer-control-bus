(ns job-streamer.control-bus.core
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

(defn wrap-same-origin-policy [handler]
  (fn [req]
    (if (= (:request-method req) :options)
      ;;Pre-flight request
      {:status 200
       :headers {"Access-Control-Allow-Methods" "POST,GET,PUT,DELETE,OPTIONS"
                 "Access-Control-Allow-Origin" "*"
                 "Access-Control-Allow-Headers" "Content-Type"}}
      (when-let [resp (handler req)]
        (header resp "Access-Control-Allow-Origin" "*")))))

(def banner "
    __     _   _____ _                 Control Bus
 __|  |___| |_|   __| |_ ___ ___ ___ _____ ___ ___ 
|  |  | . | . |__   |  _|  _| -_| .'|     | -_|  _|
|_____|___|___|_____|_| |_| |___|__,|_|_|_|___|_|
")

(defn -main [& args]
  (init)
  (let [port (Integer/parseInt (or (:control_bus-port env) "45102"))]
    (broadcast/start port)
    (go (scheduler/start "localhost" port))
    (dispatcher/start)
    
    (go-loop []
      (let [jobs (job/find-undispatched)]
        (doseq [[execution-request job parameter] jobs]
          (dispatcher/submit {:request-id execution-request
                              :class-loader-id (:application/class-loader-id (apps/find-by-name "default"))
                              :job job
                              :parameters parameter}))
        (<! (timeout 2000))
        (recur)))
    (ag/start-monitor)

    (go-loop []
      (<! (timeout 10000))
      (try
        (recovery/update-job-status)
        (catch Throwable t
          (log/error t)))
      (recur))

    (server/run-server
     (-> app-routes
         (wrap-defaults api-defaults)
         (wrap-same-origin-policy)
         wrap-reload)
     :port port
     :websockets [{:path "/join"
                   :on-message (fn [ch message]
                                 (handle-command (edn/read-string message) ch))
                   :on-close (fn [ch close-reason]
                               (log/info "disconnect" ch "for" close-reason)
                               (handle-command {:command :bye} ch))}])

    (println banner)))
