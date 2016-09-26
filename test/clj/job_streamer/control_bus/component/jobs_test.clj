(ns job-streamer.control-bus.component.jobs-test
  (:require (job-streamer.control-bus.component [jobs :as jobs]
                                                [apps :as apps]
                                                [datomic :refer [datomic-component]]
                                                [migration :refer [migration-component]])
            (job-streamer.control-bus [system :as system]
                                      [model :as model]
                                      [config :as config])
            [com.stuartsierra.component :as component]
            [meta-merge.core :refer [meta-merge]]
            [clojure.test :refer :all]
            [clojure.pprint :refer :all]
            [clojure.edn :as edn]
            [clj-time.format :as f]))



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
       :jobs    (jobs/jobs-component (:jobs config))
       :datomic (datomic-component   (:datomic config))
       :migration (migration-component {:dbschema model/dbschema}))
      (component/system-using
       {:jobs [:datomic :migration]
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

(deftest find-all
  (testing "find-all"
    (let [system (new-system config)]
      (let [res (jobs/find-all (:jobs system) "default" nil)]
        (is (= 0 (:hits res)))
        (is (empty? (:results res)))))))

(deftest list-resource
  (let [system (new-system config)
        handler (-> (jobs/list-resource (:jobs system) "default"))]
    (create-app system)
    (testing "no jobs"
      (let [request {:request-method :get}]
        (is (= 0 (-> (handler request) :body edn/read-string :hits)))))
    (testing "validation error"
      (let [request {:request-method :post
                     :content-type "application/edn"
                     :body (pr-str {})}]
        (is (= ["name must be present"]
               (-> (handler request) :body edn/read-string :messages)))))
    (testing "create a job"
      (let [request {:request-method :post
                     :content-type "application/edn"
                     :body (pr-str {:job/name "job1"})}]
        (is (= 201 (-> (handler request) :status))))
      (let [res (jobs/find-all (:jobs system) "default" nil)]
        (is (= 1 (:hits res)))))))

(deftest parse-query
  (testing "parse-query"
    (let [result (jobs/parse-query "a b since:2016-09-01 until:2016-09-02 exit-status:COMPLETED")]
      (is (= "a" (first (:job-name result))))
      (is (= "2016-09-01" (f/unparse (:date f/formatters) (:since result))))
      (is (= "2016-09-02" (f/unparse (:date f/formatters) (:until result))))
      (is (=  "COMPLETED" (:exit-status result)))))
  (testing "nil query returns nil"
    (let [result (jobs/parse-query nil)]
      (is (nil? result))))
  (testing "empty query returns nil"
    (let [result (jobs/parse-query "")]
      (is (nil? result))))

  (testing "single simple query"
    (let [result (jobs/parse-query "a")]
      (is (= {:job-name '("a")} result))))

  (testing "single simple query"
    (let [result (jobs/parse-query "a")]
      (is (= {:job-name '("a")} result))))

  (testing "ignore breaking tokens in a query"
    (let [result (jobs/parse-query "a since: until: since:xxx until:yyy")]
      (is (= {:job-name '("a")} result)))))
