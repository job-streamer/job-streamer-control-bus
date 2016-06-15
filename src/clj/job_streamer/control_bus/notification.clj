(ns job-streamer.control-bus.notification
  (:refer-clojure :exclude [send])
  (:require [org.httpkit.client :as http]
            [environ.core :refer [env]]
            [clojure.tools.logging :as log]))

(def notification-server (get env :notificator-url "http://localhost:2121"))

(defn send [type message]
  (http/post (str notification-server "/" type)
             {:headers {"content-type" "application/edn"}
              :body (pr-str message)}))

