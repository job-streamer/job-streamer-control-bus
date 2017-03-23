(ns job-streamer.control-bus.component.apps
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.string :refer [ends-with?]]
            [com.stuartsierra.component :as component]
            [liberator.core :as liberator]
            [bouncer.validators :as v]
            (job-streamer.control-bus [validation :refer [validate]]
                                      [util :refer [parse-body]])
            (job-streamer.control-bus.component [datomic :as d]
                                                [agents :as ag]))
  (:import [java.net URL]
           [net.unit8.wscl ClassLoaderHolder]))

(defn tracer-bullet-fn [])

(defn find-by-name [{:keys [applications]} name]
  (get @applications name))

(defn classpath [loader]
  (distinct
   (mapcat #(seq (.getURLs %))
           (take-while identity
                       (iterate
                        #(.getParent ^ClassLoader %) loader)))))

(defn register-app [applications app]
  (let [id (.registerClasspath
            (ClassLoaderHolder/getInstance)
            (into-array URL
                        (map #(URL. %) (:application/classpaths app)))
            (.getClassLoader (class tracer-bullet-fn)))]
    (swap! applications assoc (:application/name app)
           (assoc app
                  :application/name "default"
                  :application/class-loader-id id))
    (log/info "Registered an application [" app "]")))


(defn scan-components [classpaths]
  (let [fs (System/getProperty "file.separator")]
    (when-let [java-cmd (some-> (System/getProperty "java.home") (str fs "bin" fs "java"))]
      (log/info "Scan batch components...")
      (let [this-cps (classpath (clojure.lang.RT/baseLoader))
            cmd (into-array (concat [java-cmd
                                     "-cp"
                                     (->> (vec this-cps)
                                          (map io/file)
                                          (clojure.string/join (if (= fs "\\") ";" ":")))
                                     "net.unit8.job_streamer.control_bus.BatchComponentScanner"]
                                    classpaths))
            proc (.exec (Runtime/getRuntime) cmd)]
        (log/debug (clojure.string/join " " cmd))
        (letfn [(print-err []
                      (with-open [rdr (io/reader (.getErrorStream proc))]
                        (->> (line-seq rdr)
                             (map #(log/error %))
                             doall)))]
          (.start (Thread. print-err)))
        (with-open [rdr (io/reader (.getInputStream proc))]
          (->> (line-seq rdr)
               (map #(clojure.string/split % #":" 2))
               (map (fn [[category class-name]]
                      (let [c {(keyword "batch-component" category) class-name}]
                        (log/debugf "%s was scaned." (pr-str c)) c)))
               (reduce #(merge-with conj %1 %2)
                       {:batch-component/batchlet []
                        :batch-component/item-reader []
                        :batch-component/item-writer []
                        :batch-component/item-processor []
                        :batch-component/listener []
                        :batch-component/throwable []})))))))

(defn- find-batch-component [datomic app-name]
  (d/query datomic
           '{:find [?c .]
             :in [$ ?app-name]
             :where [[?c :batch-component/application ?app]
                     [?app :application/name ?app-name]]}
           app-name))

(defn list-resource [{:keys [datomic applications]}]
  (liberator/resource
   :available-media-types ["application/edn" "application/json"]
   :allowed-methods [:get :post]
   :malformed? #(validate (parse-body %)
                          :application/name v/required
                          :application/description v/required
                          :application/classpaths v/required)
   :post! (fn [{app :edn}]
            (if-let [app-id (d/query datomic
                                     '[:find ?e .
                                       :in $ ?n
                                       :where [?e :application/name ?n]]
                                     "default")]
              (d/transact datomic
                          [{:db/id app-id
                            :application/description (:application/description app)
                            :application/classpaths (:application/classpaths app)}])
              (d/transact datomic
                          [{:db/id #db/id[db.part/user -1]
                            :application/name "default" ;; Todo multi applications.
                            :application/description (:application/description app)
                            :application/classpaths (:application/classpaths app)}]))
            (register-app applications (assoc app :application/name "default"))
            (when-let [components (scan-components (:application/classpaths app))]
              (let [batch-component-id (find-batch-component datomic "default")]
                (when batch-component-id
                  (d/transact datomic
                              [[:db.fn/retractEntity batch-component-id]]))
                (d/transact datomic
                            [ (merge {:db/id (or batch-component-id
                                                (d/tempid :db.part/user))
                                     :batch-component/application [:application/name "default"]}
                                    components)]))))
   :handle-ok (fn [ctx]
                (vals @applications))))

(defn batch-components-resource [{:keys [datomic applications]} app-name]
  (liberator/resource
   :available-media-types ["application/edn" "application/json"]
   :allowed-methods [:get :post]
   :malformed? #(let [{:keys [request-method params]} (:request %)]
                  ;; Check the extension of the posted file.
                  (and (= request-method :post)
                       (some-> params
                               (get-in ["file" :filename])
                               (ends-with? ".jar")
                               not)))
   :post! (fn [ctx]
            (let [{:keys [filename tempfile size]} (get-in ctx [:request :params "file"])
                  jar-file (io/file "batch-components" app-name filename)
                  classpaths [(.toString (io/as-url jar-file))]
                  description "Uploaded by the console."]
              (log/infof "Jar file is being uploaded [%s, %d bytes]" filename size)
              (io/make-parents jar-file)
              (io/copy tempfile jar-file)
              (if-let [app-id (d/query datomic
                                     '[:find ?e .
                                       :in $ ?n
                                       :where [?e :application/name ?n]]
                                     app-name)]
                (d/transact datomic
                            [{:db/id app-id
                              :application/description description
                              :application/classpaths classpaths}])
                (d/transact datomic
                            [{:db/id #db/id[db.part/user -1]
                              :application/name app-name
                              :application/description description
                              :application/classpaths classpaths}]))
              (register-app applications {:application/name app-name
                                          :application/classpaths classpaths
                                          :application/description description})
              (when-let [components (scan-components classpaths)]
                (let [batch-component-id (find-batch-component datomic app-name)]
                  (when batch-component-id
                    (d/transact datomic
                                [[:db.fn/retractEntity batch-component-id]]))
                  (d/transact datomic
                              [ (merge {:db/id (or batch-component-id
                                                  (d/tempid :db.part/user))
                                       :batch-component/application [:application/name app-name]}
                                      components)])))))
   :handle-ok (fn [ctx]
                (let [in-app (->> (d/query datomic
                                           '{:find [?c .]
                                             :in [$ ?app-name]
                                             :where [[?c :batch-component/application ?app]
                                                     [?app :application/name ?app-name]]}
                                           app-name)
                                  (d/pull datomic '[:*]))
                      builtins {:batch-component/batchlet
                                ["org.jobstreamer.batch.ShellBatchlet", "org.jobstreamer.batch.JavaMainBatchlet"]

                                :batch-component/item-writer []
                                :batch-component/item-processor []
                                :batch-component/item-reader []
                                :batch-component/listener []}]
                  (merge-with #(vec (concat %1 %2))  builtins in-app)))))

(defn stats-resource [{:keys [datomic agents]} app-name]
  (liberator/resource
   :available-media-types ["application/edn" "application/json"]
   :allowed-methods [:get]
   :handle-ok (fn [ctx]
                {:agents (count (ag/available-agents agents))
                 :jobs   (or (d/query datomic
                                      '{:find [(count ?job) .]
                                        :in [$ ?app-name]
                                        :where [[?app :application/name ?app-name]
                                                [?app :application/jobs ?job]]}
                                      app-name) 0)})))

(defrecord BatchApplications [datomic]
  component/Lifecycle

  (start [component]
    (when-not (d/query datomic
                       '[:find ?e .
                         :in $ ?n
                         :where [?e :application/name ?n]]
                       "default")
      (d/transact datomic
                  [{:db/id #db/id[db.part/user -1]
                    :application/name "default"
                    :application/description "default application"
                    :application/classpaths []
                    :application/members []}]))
    (let [applications (atom {})
          apps (d/query (:datomic component)
                        '{:find [[(pull ?app [*])]]
                          :where [[?app :application/name]]})]
      (doseq [app apps]
        (register-app applications app))

      (assoc component
             :applications applications)))

  (stop [component]
    (dissoc component :applications)))

(defn apps-component [& options]
  (map->BatchApplications options))
