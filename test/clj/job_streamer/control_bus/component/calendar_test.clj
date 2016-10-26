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
                     :body (pr-str {:calendar/name "cal1"
                                    :calendar/weekly-holiday [true false false false false false true]
                                    :calendar/holidays []})}]
        (is (= 201 (-> (handler request) :status)))))
    (testing "get one calendar"
      (let [request {:request-method :get}
            res (-> (handler request) :body edn/read-string)]
        (is (= 1 (count res)))
        (is (= "cal1" (-> res first :calendar/name)))))))


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
