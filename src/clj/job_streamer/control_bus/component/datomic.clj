(ns job-streamer.control-bus.component.datomic
  (:require [datomic.api :as d]
            [meta-merge.core :refer [meta-merge]]
            [com.stuartsierra.component :as component]))

(defn tempid [part]
  (d/tempid part))

(defprotocol IDataSource
  (query*   [this q params])
  (pull     [this pattern eid])
  (transact [this transaction])
  (resolve-tempid [this tempids tempid]))

(defn query [this q & params]
  (query* this q params))

(defrecord DatomicDataSource [uri recreate?]
  component/Lifecycle

  (start [component]
    (if (:connection component)
      component
      (do (when recreate?
            (d/delete-database uri))
          (d/create-database uri)
          (assoc component :connection (d/connect uri)))))

  (stop [component]
    (dissoc component :connection))

  IDataSource

  (query* [{:keys [connection]} q params]
    (let [db (d/db connection)]
      (apply d/q q db params)))

  (pull [{:keys [connection]} pattern eid]
    (let [db (d/db connection)]
      (d/pull db pattern eid)))

  (transact [{:keys [connection]} transaction]
    @(d/transact connection transaction))

  (resolve-tempid [{:keys [connection]} tempids tempid]
    (let [db (d/db connection)]
      (d/resolve-tempid db tempids tempid))))

(defn datomic-component [options]
  (let [uri (:uri options "datomic:free://localhost:4334/job-streamer")]
    (map->DatomicDataSource (meta-merge options {:uri uri}))))
