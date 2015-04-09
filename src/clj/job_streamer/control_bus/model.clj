(ns job-streamer.control-bus.model
  (:use [datomic-schema.schema :only [fields part schema]]
        [environ.core :only [env]])
  (:require [datomic.api :as d]
            [datomic-schema.schema :as s]))

(def uri (or (env :datomic-url "datomic:free://localhost:4334/job-streamer")))
(defonce conn (atom nil))

(defn query [q & params]
  (let [db (d/db @conn)]
    (apply d/q q db params)))

(defn pull [pattern eid]
  (let [db (d/db @conn)]
    (d/pull db pattern eid)))

(defn transact [transaction]
  @(d/transact @conn transaction))

(defn dbparts []
  [(part "job")])

(defn dbschema []
  [(schema application
           (fields
            [name :string :indexed :unique-value :fulltext]
            [description :string]
            [classpaths :string :many]
            [jobs :ref :many]))
   (schema batch-component
           (fields
            [application :ref]
            [batchlet :string :many]
            [item-reader :string :many]
            [item-writer :string :many]
            [item-processor :string :many]
            [throwable :string :many]))
   (schema job
           (fields
            [name :string  :indexed :fulltext]
            [restartable? :boolean]
            [steps :ref :many]
            [edn-notation :string]
            [schedule :ref]
            [executions :ref :many]))
   (schema step
           (fields
            [name :string :indexed]
            [start-limit :long]
            [allow-start-if-complete? :boolean]
            [chunk :ref]
            [batchlet :ref]))
   (schema chunk
           (fields
            [checkpoint-policy :enum [:item :custom]]
            [item-count  :long]
            [time-limit  :long]
            [skip-limit  :long]
            [retry-limit :long]))
   (schema batchlet
           (fields
            [ref :string]))
   (schema agent
           (fields
            [instance-id :uuid :unique-value :indexed]
            [name :string]))
   (schema job-execution
           (fields
            [create-time :instant]
            [start-time  :instant]
            [end-time    :instant]
            [job-parameters :string]
            [batch-status :ref]
            [agent :ref]
            [execution-id :long]
            [step-executions :ref :many]))
   (schema step-execution
           (fields
            [step :ref]
            [step-execution-id :long]
            [start-time :instant]
            [end-time   :instant]
            [batch-status :ref]
            [execution-exception :string]))
   (schema execution-log
           (fields
            [date :instant]
            [agent :ref]
            [step-execution-id :long]
            [logger :string]
            [level :ref]
            [message :string :fulltext :indexed]
            [exception :string]))
   (schema schedule
           (fields
            [active? :boolean]
            [cron-notation :string]))])

(defn generate-enums [& enums]
  (apply concat
         (map #(s/get-enums (name (first %)) :db.part/user (second %)) enums)))

(defn create-schema []
  (d/create-database uri)
  (reset! conn (d/connect uri))
  (let [schema (concat
                (s/generate-parts (dbparts))
                (generate-enums [:batch-status [:undispatched :queued :abandoned :completed :failed :started :starting :stopped :stopping :unknown]]
                                [:log-level [:trace :debug :info :warn :error]])
                (s/generate-schema (dbschema)))]
    (d/transact
     @conn
     schema)))

