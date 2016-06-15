(ns job-streamer.control-bus.component.datomic
  (:require [datomic.api :as d]
            [com.stuartsierra.component :as component]))

(defn query [{:keys [connection]} q & params]
  (let [db (d/db connection)]
    (apply d/q q db params)))

(defn pull [{:keys [connection]} pattern eid]
  (let [db (d/db connection)]
    (d/pull db pattern eid)))

(defn transact [{:keys [connection]} transaction]
  @(d/transact connection transaction))

(defn resolve-tempid [{:keys [connection]} tempids tempid]
  (let [db (d/db connection)]
    (d/resolve-tempid db tempids tempid)))

(defn tempid [part]
  (d/tempid part))

(defrecord DatomicDataSource [uri]
  component/Lifecycle

  (start [component]
    (d/create-database uri)
    (assoc component :connection (d/connect uri)))

  (stop [component]
    (dissoc component :connection)))

(defn datomic-component [options]
  (let [uri (:uri options "datomic:free://localhost:4334/job-streamer")]
    (map->DatomicDataSource {:uri uri})))

