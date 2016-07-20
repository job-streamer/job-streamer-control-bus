(ns job-streamer.control-bus.component.migration
  (:require [com.stuartsierra.component :as component]
            [datomic.api :refer [part]]
            [datomic-schema.schema :as s]
            (job-streamer.control-bus.component [datomic :as d])))

(defn- generate-enums [& enums]
  (apply concat
         (map #(s/get-enums (name (first %))
                            :db.part/user (second %)) enums)))

(defn- dbparts []
  [(part "job")])

(defrecord Migration [datomic dbschema]
  component/Lifecycle

  (start [component]
    (let [schema (concat
                  ;(s/generate-parts (dbparts))
                  (generate-enums [:batch-status [:undispatched :unrestarted :queued
                                                  :abandoned :completed :failed
                                                  :started :starting :stopped :stopping
                                                  :unknown]]
                                  [:log-level [:trace :debug :info :warn :error]]
                                  [:action [:abandon :stop :alert]])
                  (s/generate-schema dbschema))]
      (d/transact datomic schema)
      component))

  (stop [component]
    component))

(defn migration-component [options]
  (map->Migration options))
