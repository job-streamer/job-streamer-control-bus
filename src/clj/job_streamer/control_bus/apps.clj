(ns job-streamer.control-bus.apps
  (:require [clojure.tools.logging :as log])
  (:import [java.net URL]
           [net.unit8.wscl ClassLoaderHolder]))

(defonce applications (atom {}))

(defn tracer-bullet-fn [])

(defn register [app]
  (let [id (.registerClasspath
            (ClassLoaderHolder/getInstance)
            (into-array URL
                        (map #(URL. %) (:application/classpaths app)))
            (.getClassLoader (class tracer-bullet-fn)))]
    (swap! applications
           assoc
           (:application/name app)
           (assoc app :application/class-loader-id id))
    (log/info "Registered an application [" app "]")))

(defn find-by-name [name]
  (get @applications name))
