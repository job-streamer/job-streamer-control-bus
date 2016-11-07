(ns job-streamer.control-bus.component.calendar-test
  (:require (job-streamer.control-bus.component [calendar :as calendar]
                                                [jobs :as jobs]
                                                [scheduler :as scheduler]
                                                [apps :as apps]
                                                [datomic :refer [datomic-component] :as d]
                                                [migration :refer [migration-component]])
            (job-streamer.control-bus [system :as system]
                                      [model :as model]
                                      [config :as config])
            [com.stuartsierra.component :as component]
            [meta-merge.core :refer [meta-merge]]
            [clojure.test :refer :all]
            [clojure.pprint :refer :all]
            [clojure.edn :as edn]
            [clj-time.format :as f])
  (:import [org.joda.time DateTime]))

(def test-config
  {:datomic {:recreate? true
             :uri "datomic:mem://test"}})

(def config
  (meta-merge config/defaults
              config/environ
              test-config))

(defn new-system [config]
  (-> (component/system-map
       :apps    (apps/apps-component (:apps config))
       :calendar    (calendar/calendar-component (:calendar config))
       :scheduler (scheduler/scheduler-component (:scheduler config))
       :datomic (datomic-component   (:datomic config))
       :migration (migration-component {:dbschema model/dbschema}))
      (component/system-using
       {:calendar [:datomic :migration :scheduler]
        :scheduler [:datomic]
        :apps [:datomic]
        :migration [:datomic]})
      (component/start-system)))

(defn create-app [system]
  (let [handler (-> (apps/list-resource (:apps system)))
        request {:request-method :post
                 :content-type "application/edn"
                 :body (pr-str {:application/name "default"
                                :application/description "default application"
                                :application/classpaths []})}]
    (handler request)))

(deftest list-resource
  (let [system (new-system config)
        handler (-> (calendar/list-resource (:calendar system)))]
    (create-app system)
    (testing "no calendars"
      (let [request {:request-method :get}]
        (is (= [] (-> (handler request) :body edn/read-string)))))
    (testing "name is required"
      (let [request {:request-method :post
                     :content-type "application/edn"
                     :body (pr-str {:calendar/name ""})}]
        (is (= 400 (-> (handler request) :status) ))))
    (testing "create calendars"
      (let [request {:request-method :post
                     :content-type "application/edn"
                     :body (pr-str {:calendar/name "cal"
                                    :calendar/weekly-holiday [true false false false false false true]
                                    :calendar/holidays []
                                    :calendar/day-start "02:00"})}]
        (is (= 201 (-> (handler request) :status)))))
    (testing "get one calendar"
      (let [request {:request-method :get}
            res (-> (handler request) :body edn/read-string)]
        (is (= 1 (count res)))
        (is (= "cal" (-> res first :calendar/name)))
        (is (= "02:00" (-> res first :calendar/day-start)))))))

(deftest download-list-resource
  (let [system (new-system config)
        handler (-> (-> (calendar/list-resource (:calendar system) :download? true)))]
    (testing "export a calendar"
      (let [request {:request-method :post
                     :content-type "application/edn"
                     :body (pr-str {:calendar/name "cal1"
                                    :calendar/weekly-holiday [true false false false false false true]
                                    :calendar/holidays [(.toDate (f/parse (:date f/formatters) "2016-09-01"))]
                                    :calendar/day-start "02:00"})}]
        (is (= 201 (-> ((-> (calendar/list-resource (:calendar system))) request) :status))))
      (let [request {:request-method :get}
            response (handler request)]
        (is (= "cal1" (-> response :body read-string first :calendar/name)))
        (is (= "application/force-download"  ((:headers response) "Content-Type")))
        (is (= "attachment; filename=\"cals.edn\""  ((:headers response) "Content-disposition")))))
    (testing "before export and after import are same"
      (let [request {:request-method :post
                     :content-type "application/edn"
                     :body (pr-str {:calendar/name "cal2"
                                    :calendar/weekly-holiday [true false false false false false true]
                                    :calendar/holidays []})}]
        (is (= 201 (-> ((-> (calendar/list-resource (:calendar system))) request) :status))))
      (let [request {:request-method :get}
            before-response (handler request)
            delete-request{:request-method :delete}]
        ((-> (calendar/entry-resource (:calendar system) "cal1")) delete-request)
        ((-> (calendar/entry-resource (:calendar system) "cal2")) delete-request)
        (for [cal (:body before-response)]
          ((-> (calendar/list-resource (:calendar system)))
           {:request-method :post
            :content-type "application/edn"
            :body cal}))
        (is before-response (handler request))))))

(deftest parse-sort-order
  (testing "parse-query-name-asc"
    (let [result (calendar/parse-sort-order "name:asc")]
      (is (= :asc (:name result)))))
  (testing "parse-query-name-desc"
    (let [result (calendar/parse-sort-order "name:asc")]
      (is (= :asc (:name result))))))

(deftest sort-by-map
  (testing "sort-by-map"
    (let [res (calendar/sort-by-map
                {:name :asc}
                (read-string "({:calendar/name \"calendar1\"},{:calendar/name \"calendar2\"},{:calendar/name \"calendar3\"})"))]
      (is (= "calendar1" (-> res first :calendar/name)))
      (is (= "calendar2" (-> res second :calendar/name)))
      (is (= "calendar3" (-> res (nth 2) :calendar/name))))
    (let [res (calendar/sort-by-map
                {:name :desc}
                (read-string "({:calendar/name \"calendar1\"},{:calendar/name \"calendar2\"},{:calendar/name \"calendar3\"})"))]

      (is (= "calendar1" (-> res (nth 2) :calendar/name)))
      (is (= "calendar2" (-> res second :calendar/name)))
      (is (= "calendar3" (-> res first :calendar/name))))))
