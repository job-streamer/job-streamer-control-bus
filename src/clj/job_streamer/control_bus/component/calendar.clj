(ns job-streamer.control-bus.component.calendar
  (:require [clojure.edn :as edn]
            [liberator.core :as liberator]
            [liberator.representation :refer [ring-response]]
            [bouncer.core :as b]
            [bouncer.validators :as v]
            [clojure.string :as str]
            [com.stuartsierra.component :as component]
            [ring.util.response :refer [response content-type header]]
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

(defn- decide-sort-order [asc-or-desc v1 v2]
  (if (= :asc asc-or-desc)
    (compare v1 v2)
    (compare v2 v1)))


(defn- parse-sort-order-component [query]
  (let [split (str/split query #":")
        name (first split)
        sort-order (some-> split second .toLowerCase)]
    (when (and
            (b/valid? {:sort-order sort-order}
                      :sort-order [[v/member #{
                                                "asc"
                                                "desc"}]])
            (b/valid? {:name name}
                      :name [[v/member #{
                                          ;now name only
                                          "name"
                                          }]]))
      {(keyword name) (keyword sort-order)})))

(defn parse-sort-order [query]
  (when (not-empty query)
    (->> (str/split query #",")
         (map #(parse-sort-order-component %))
         (apply merge))))

(defn sort-by-map [sort-order-map result]
  (if (empty? sort-order-map)
    result
    (loop [sort-order-vector (reverse (seq sort-order-map))
           sorted-result result]
      (if-let [sort-order (first sort-order-vector)]
        (recur (rest sort-order-vector)
               (cond->> sorted-result
                        ;now name only
                        (= :name (first sort-order))
                        (sort-by :calendar/name (fn [v1 v2] (decide-sort-order (second sort-order) v1 v2)))))
        sorted-result))))

(defn find-calendar-by-name [{:keys [datomic]} cal-name]
  (d/query datomic
           '{:find [?calendar .]
             :in [$ ?calendar-name]
             :where [[?calendar :calendar/name ?calendar-name]]}
           cal-name))

(defn list-resource
  [{:keys [datomic scheduler] :as component} & {:keys [download?] :or {download? false}}]
  (liberator/resource
   :available-media-types ["application/edn" "application/json"]
   :allowed-methods [:get :post]
   :malformed? #(validate (parse-body %)
                          :calendar/name v/required
                          :calendar/day-start [[(fn [val] (or (empty? val) (scheduler/hh:MM? val)))  :message "Invalid hh:MM format"]])
   :exists? (fn [{{cal-name :calendar/name} :edn :as ctx}]
              (if (#{:post} (get-in ctx [:request :request-method]))
                (when-let [id (find-calendar-by-name component cal-name)]
                  {:cal-id id})
                true))

   :post! (fn [{cal :edn :as ctx}]
            (d/transact datomic
                        [{:db/id (or (:cal-id ctx) (d/tempid :db.part/user))
                          :calendar/name (:calendar/name cal)
                          :calendar/holidays (:calendar/holidays cal [])
                          :calendar/weekly-holiday (pr-str (:calendar/weekly-holiday cal))
                          :calendar/day-start (:calendar/day-start cal "00:00")}])
            (when-not (:cal-id ctx)
              (scheduler/add-calendar scheduler cal)))

   :handle-ok (fn [{{{sort-order :sort-by} :params} :request}]
                (let [res (->> (d/query datomic
                              '{:find [[(pull ?cal [:*]) ...]]
                                :in [$]
                                :where [[?cal :calendar/name]]})
                               (map (fn [cal]
                                      (update-in cal [:calendar/weekly-holiday]
                                                 edn/read-string)))
                               (sort-by-map (parse-sort-order sort-order))
                               vec)]
                  (if download?
                    (-> (map #(dissoc % :db/id) res)
                        pr-str
                        response
                        (content-type "application/force-download")
                        (header "Content-disposition" "attachment; filename=\"cals.edn\"")
                        (ring-response))
                    res)))))

(defn entry-resource [{:keys [datomic scheduler]} name]
  (liberator/resource
   :available-media-types ["application/edn" "application/json"]
   :allowed-methods [:get :put :delete]
   :malformed? (fn [ctx]
                 (or (validate (parse-body ctx)
                               :calendar/name v/required
                               :calendar/day-start [[(fn [val] (or (empty? val) (scheduler/hh:MM? val)))  :message "Invalid hh:MM format"]])
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
                         :calendar/weekly-holiday (pr-str (:calendar/weekly-holiday cal))
                         :calendar/day-start (:calendar/day-start cal "00:00")}]))
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
