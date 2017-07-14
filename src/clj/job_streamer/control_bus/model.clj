(ns job-streamer.control-bus.model
  (:require [datomic-schema.schema :refer [fields part schema]]))

(def dbschema
  [(schema application
           (fields
            [name :string :indexed :unique-value :fulltext]
            [description :string]
            [classpaths :string :many]
            [jobs :ref :many]
            [members :ref :many]))
   (schema batch-component
           (fields
            [application :ref]
            [batchlet :string :many]
            [item-reader :string :many]
            [item-writer :string :many]
            [item-processor :string :many]
            [listener :string :many]
            [throwable :string :many]))
   (schema job
           (fields
            [name :string  :indexed :fulltext]
            [bpmn-xml-notation :string]
            [svg-notation :string]
            [schedule :ref]
            [exclusive? :boolean]
            [time-monitor :ref]
            [status-notifications :ref :many]
            [executions :ref :many]))
   (schema time-monitor
           (fields
            [duration :long]
            [action :ref]
            [notification-type :string]))
   (schema status-notification
           (fields
            [batch-status :ref]
            [exit-status  :string]
            [type :string]))
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
            [exit-status :string]
            [agent :ref]
            [execution-id :long]
            [step-executions :ref :many]))
   (schema step-execution
           (fields
            [step-name :string]
            [step-execution-id :long]
            [start-time :instant]
            [end-time   :instant]
            [batch-status :ref]
            [exit-status :string]
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
            [cron-notation :string]
            [calendar :ref]))
   (schema calendar
           (fields
            [name :string :unique-value :indexed]
            [weekly-holiday :string]
            [holidays :instant :many]
            [day-start :string]))
   (schema member
           (fields
             [user :ref]
             [roles :ref :many]))
   (schema user
           (fields
             [id :string :indexed :unique-value]
             [password :string]
             [salt :bytes]
             [token :string]))
   (schema role
           (fields
             [name :string :indexed :unique-value]
             [permissions :keyword :many]))])
