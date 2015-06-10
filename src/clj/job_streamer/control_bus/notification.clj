(ns job-streamer.control-bus.notification
  (:use [environ.core :only [env]])
  (:require [org.httpkit.client :as http]
            [clojure.tools.logging :as log]))

(def notification-server (get env :notification-server "http://localhost:2121"))
(defn send [type message]
  (http/post (str notification-server "/" type)
             {:headers {"content-type" "application/edn"}
              :body (pr-str message)}))

