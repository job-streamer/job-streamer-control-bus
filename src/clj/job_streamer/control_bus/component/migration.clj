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

(defn- migration-v1 [datomic dbschemas]
  (log/info "Start migration-v1.")
  (let [schema (concat
                  ;(s/generate-parts (dbparts))
                  (generate-enums [:batch-status [:undispatched :unrestarted :queued
                                                  :abandoned :completed :failed
                                                  :started :starting :stopped :stopping
                                                  :unknown]]
                                  [:log-level [:trace :debug :info :warn :error]]
                                  [:action [:abandon :stop :alert]])
                  (s/generate-schema (apply concat dbschemas)))]
    (d/transact datomic schema)
    (log/info "Create an initial app, user and roles.")
    (d/transact datomic [{:db/id (d/tempid :db.part/user)
                          :application/name "default"
                          :application/description "default application"
                          :application/classpaths []
                          :application/members []}
                         {:db/id (d/tempid :db.part/user)
                          :role/name "admin"
                          :role/permissions [:permission/read-job
                                             :permission/create-job
                                             :permission/update-job
                                             :permission/delete-job
                                             :permission/execute-job]}
                         {:db/id (d/tempid :db.part/user)
                          :role/name "operator"
                          :role/permissions [:permission/read-job
                                             :permission/execute-job]}
                         {:db/id (d/tempid :db.part/user)
                          :role/name "watcher"
                          :role/permissions [:permission/read-job]}
                         {:db/id (d/tempid :db.part/user) :schema/version 2}])
    (signup datomic {:user/id "admin" :user/password "password123"} "admin")
    (signup datomic {:user/id "guest" :user/password "password123"} "operator")
    (log/info "Succeeded migration-v1.")))

(defn- migration-v2 [datomic dbschemas]
  (log/info "Start migration-v2.")
  (let [schema (s/generate-schema (second dbschemas))
        alteration [{:db/id :roll/name
                     :db/ident :role/name}
                    {:db/id :roll/permissions
                     :db/ident :role/permissions}
                    {:db/id :member/rolls
                     :db/ident :member/roles}]
        version [{:db/id (d/tempid :db.part/user) :schema/version 2}]]
    (d/transact datomic (concat schema alteration))
    (d/transact datomic version)
    (log/info "Succeeded migration-v2.")))

(defn- find-role-id [datomic name]
  (d/query datomic
           '{:find [?r .]
             :in [$ ?n]
             :where [[?r :role/name ?n]]}
           name))

(defn- find-schema-id [datomic]
  (d/query datomic
           '{:find [?s .]
             :in [$]
             :where [[?s :schema/version]]}))

(defn- find-schema-version [datomic]
  (d/query datomic
           '{:find [?v .]
             :in [$]
             :where [[?s :schema/version ?v]]}))

(defn- migration-v3 [datomic]
  (log/info "Start migration-v3.")
  (d/transact datomic [{:db/id (find-role-id datomic "admin")
                        :role/permissions [:permission/read-calendar
                                           :permission/create-calendar
                                           :permission/update-calendar
                                           :permission/delete-calendar]}
                       {:db/id (find-role-id datomic "operator")
                        :role/permissions [:permission/read-calendar
                                           :permission/create-calendar
                                           :permission/update-calendar
                                           :permission/delete-calendar]}
                       {:db/id (find-role-id datomic "watcher")
                        :role/permissions [:permission/read-calendar]}
                       {:db/id (find-schema-id datomic) :schema/version 3}])
  (log/info "Succeeded migration-v3."))

(defrecord Migration [datomic dbschemas]
  component/Lifecycle

  (start [component]
    ;; Confirm schema is set
    (let [has-schema? (some? (d/query datomic
                            '[:find ?s .
                              :in $
                              :where [?s :db/ident :application/name]]))
          has-version? (some? (d/query datomic
                            '[:find ?s .
                              :in $
                              :where [?s :db/ident :schema/version]]))]
      (if-not has-schema?
        (migration-v1 datomic dbschemas)
        (when-not has-version?
          (migration-v2 datomic dbschemas))))

    (when (= 2 (find-schema-version datomic))
      (migration-v3 datomic))

    (log/info "schema version" (find-schema-version datomic))
    component)

  (stop [component]
    component))

(defn migration-component [options]
  (map->Migration options))
