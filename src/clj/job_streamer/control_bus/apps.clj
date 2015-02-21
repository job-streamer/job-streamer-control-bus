(ns job-streamer.control-bus.apps
  (:import [java.net URL]
           [net.unit8.wscl ClassLoaderHolder]))

(defonce applications (atom {}))

(defn tracer-bullet-fn [])

(defn register [app]
  (let [id (.registerClasspath
            (ClassLoaderHolder/getInstance)
            (into-array URL
                        (map #(URL. %) (:classpaths app)))
            (.getClassLoader (class tracer-bullet-fn)))]
    (swap! applications assoc (:name app) (assoc app :id id))))

(defn find-by-name [name]
  (get @applications name))
