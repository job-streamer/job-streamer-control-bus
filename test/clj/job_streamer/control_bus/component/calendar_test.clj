(ns job-streamer.control-bus.component.calendar-test
  (:require (job-streamer.control-bus.component [calendar :as calendar]
                                                [jobs :as jobs]
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
