(ns job-streamer.control-bus.component.migration
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log]
            [datomic.api :refer [part]]
            [datomic-schema.schema :as s]
            (job-streamer.control-bus.component [datomic :as d]
                                                [auth :refer [signup]])))

(defn- generate-enums [& enums]
  (apply concat
         (map #(s/get-enums (name (first %))
                            :db.part/user (second %)) enums)))

(defn- dbparts []
  [(part "job")])

(defn- first-migration [datomic dbschema]
  (log/info "Start first-migration.")
  (let [schema (concat
                  ;(s/generate-parts (dbparts))
                  (generate-enums [:batch-status [:undispatched :unrestarted :queued
                                                  :abandoned :completed :failed
                                                  :started :starting :stopped :stopping
                                                  :unknown]]
                                  [:log-level [:trace :debug :info :warn :error]]
                                  [:action [:abandon :stop :alert]])
                  (s/generate-schema dbschema))
        has-schema? (some? (d/query datomic
                                    '[:find ?s .
                                      :in $
                                      :where [?s :db/ident :application/name]]))]
        (d/transact datomic schema)
        (d/transact datomic [{:db/id (d/tempid :db.part/user) :schema/version 1}])
        (when-not has-schema?
          (log/info "Create an initial app, user and rolls.")
          (d/transact datomic [{:db/id (d/tempid :db.part/user)
                                :application/name "default"
                                :application/description "default application"
                                :application/classpaths []
                                :application/members []}
                               {:db/id (d/tempid :db.part/user)
                                :roll/name "admin"
                                :roll/permissions [:permission/read-job
                                                   :permission/create-job
                                                   :permission/update-job
                                                   :permission/delete-job
                                                   :permission/execute-job]}
                               {:db/id (d/tempid :db.part/user)
                                :roll/name "operator"
                                :roll/permissions [:permission/read-job
                                                   :permission/execute-job]}
                               {:db/id (d/tempid :db.part/user)
                                :roll/name "watcher"
                                :roll/permissions [:permission/read-job]}])
          (signup datomic {:user/id "admin" :user/password "password123"} "admin"))
      (log/info "Succeeded first-migration.")))

(defrecord Migration [datomic dbschema]
  component/Lifecycle

  (start [component]
    ;; Confirm schema is set
    (when (nil? (d/query datomic
                         '[:find ?s .
                           :in $
                           :where [?s :db/ident :schema/version]]))
      (first-migration datomic dbschema))
      component)

  (stop [component]
    component))

(defn migration-component [options]
  (map->Migration options))
