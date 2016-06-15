(ns job-streamer.control-bus.component.calendar
  (:require [clojure.edn :as edn]
            [liberator.core :as liberator]
            [liberator.representation :refer [ring-response]]
            [bouncer.validators :as v]
            [com.stuartsierra.component :as component]
            (job-streamer.control-bus [validation :refer (validate)]
                                      [util :refer [parse-body]])
            (job-streamer.control-bus.component [datomic :as d]
                                                [scheduler :as scheduler])))

(defn calendar-is-already-used-by-job? [datomic calendar-name]
  (d/query datomic
           '{:find [(count ?job) .]
             :in [$ ?calendar-name]
             :where [[?job :job/schedule ?schedule]
                     [?schedule :schedule/calendar ?calendar]
                     [?calendar :calendar/name ?calendar-name]]}
           calendar-name))

(defn list-resource [{:keys [datomic scheduler]}]
  (liberator/resource
   :available-media-types ["application/edn"]
   :allowed-methods [:get :post]
   :malformed? #(validate (parse-body %)
                          :calendar/name v/required)
   :exists? (fn [{{cal-name :calendar/name} :edn :as ctx}]
              (if (#{:post} (get-in ctx [:request :request-method]))
                (when-let [cal (d/query datomic
                                        '{:find [?e .]
                                          :in [$ ?n]
                                          :where [[?e :calendar/name ?n]]}
                                        cal-name)]
                  {:cal-id cal})
                true))

   :post! (fn [{cal :edn}]
            (let [id (or (:db/id cal) (d/tempid :db.part/user))]
              (if-let [old-id (d/query datomic
                                       '{:find [?calendar .]
                                         :in [$ ?calendar-name]
                                         :where [[?calendar :calendar/name ?calendar-name]]}
                                       (:calendar/name cal))]
                (d/transact datomic
                            [{:db/id old-id
                              :calendar/name (:calendar/name cal)
                              :calendar/holidays (:calendar/holidays cal)
                              :calendar/weekly-holiday (pr-str (:calendar/weekly-holiday cal))}])
                (do
                  (scheduler/add-calendar scheduler cal)
                  (d/transact datomic
                              [{:db/id id
                                :calendar/name (:calendar/name cal)
                                :calendar/holidays (:calendar/holidays cal)
                                :calendar/weekly-holiday (pr-str (:calendar/weekly-holiday cal))}])))))
   :handle-ok (fn [_]
                (->> (d/query datomic
                              '{:find [[(pull ?cal [:*]) ...]]
                                :in [$]
                                :where [[?cal :calendar/name]]})
                     (map (fn [cal]
                            (update-in cal [:calendar/weekly-holiday]
                                       edn/read-string)))))))

(defn entry-resource [{:keys [datomic scheduler]} name]
  (liberator/resource
   :available-media-types ["application/edn"]
   :allowed-methods [:get :put :delete]
   :malformed? (fn [ctx]
                 (or (validate (parse-body ctx)
                               :calendar/name v/required)
                     (and (#{:delete} (get-in ctx [:request :request-method]))
                          (when (calendar-is-already-used-by-job? datomic name)
                            {:message {:messages ["This calendar is already used by some job."]}}))))

   :exists? (d/query datomic
                     '{:find [(pull ?e [:*]) .]
                       :in [$ ?n]
                       :where [[?e :calendar/name ?n]]} name)
   
   :put! (fn [{cal :edn}]
           (d/transact datomic
                       [{:db/id (:db/id cal)
                         :calendar/name (:calendar/name cal)
                         :calendar/holidays (:calendar/holidays cal)
                         :calendar/weekly-holiday (pr-str (:calendar/weekly-holiday cal))}]))
   :delete! (fn [ctx]
              (scheduler/delete-calendar scheduler name)
              (d/transact datomic
                          [[:db.fn/retractEntity [:calendar/name name]]]))
   :handle-ok (fn [ctx]
                (some-> (d/query datomic
                                 '{:find [(pull ?e [:*]) .]
                                   :in [$ ?n]
                                   :where [[?e :calendar/name ?n]]} name)
                        (update-in [:calendar/weekly-holiday] edn/read-string)))
   :handle-malformed (fn[ctx]
                       (ring-response {:status 400
                                       :body (pr-str (:message ctx))}))))


(defrecord Calendar []
  component/Lifecycle

  (start [component]
    component)

  (stop [component]
    component))

(defn calendar-component [options]
  (map->Calendar options))
