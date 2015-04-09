(ns job-streamer.control-bus.apps
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io])
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

(defn scan-components [classpaths]
  (when-let [java-cmd (some-> (System/getProperty "java.home") (str "/bin/java"))]
    (log/info "Scan batch components...")
    (let [this-cps (.. (Thread/currentThread) getContextClassLoader getURLs)
          cmd (into-array (concat [java-cmd
                                           "-cp"
                                           (->> (vec this-cps)
                                               (map str)
                                               (clojure.string/join ":"))
                                           "net.unit8.job_streamer.control_bus.BatchComponentScanner"]
                                          classpaths))
          proc (.exec (Runtime/getRuntime) cmd)]
      (log/info (clojure.string/join " " cmd))
      (with-open [rdr (io/reader (.getInputStream proc))]
        (->> (line-seq rdr)
             (map #(clojure.string/split % #":" 2))
             (map (fn [[category class-name]]
                    {(keyword "batch-component" category) class-name}))
             (reduce #(merge-with conj %1 %2)
                     {:batch-component/batchlet []
                      :batch-component/item-reader []
                      :batch-component/item-writer []
                      :batch-component/item-processor []
                      :batch-component/throwable []}))))))
