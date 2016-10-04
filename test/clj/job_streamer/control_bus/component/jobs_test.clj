(ns job-streamer.control-bus.component.jobs-test
  (:require (job-streamer.control-bus.component [jobs :as jobs]
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

(defn setup-execution [{:keys [datomic] :as jobs}
                       {:keys [db/id job-execution/end-time job-execution/create-time]
                        :or {job-execution/end-time (java.util.Date.)
                             job-execution/create-time (java.util.Date.)}}]
  (let [execution-id (d/tempid :db.part/user)]
    (-> (d/transact
         datomic
         [{:db/id execution-id
           :job-execution/batch-status :batch-status/undispatched
           :job-execution/create-time create-time
           :job-execution/end-time end-time
           :job-execution/exit-status "COMPLETE"
           :job-execution/job-parameters "{}"}
          [:db/add id :job/executions execution-id]])
        :tempids)))

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

(deftest find-all-with-query
  (let [system (new-system config)
        handler (-> (jobs/list-resource (:jobs system) "default"))]
    (create-app system)
    ;; setup data
    (handler {:request-method :post
              :content-type "application/edn"
              :body (pr-str {:job/name "job1"})})
    (handler {:request-method :post
              :content-type "application/edn"
              :body (pr-str {:job/name "job2"})})
    (testing "Mathes exactly"
      (let [res (jobs/find-all (:jobs system) "default" "job1")]
        (is (= 1 (:hits res)))))

    (testing "A query of multiple keywords"
      (let [res (jobs/find-all (:jobs system) "default" "job1 job2")]
        (is (= 2 (:hits res)))))

    (testing "backward matching"
      (let [res (jobs/find-all (:jobs system) "default" "b2")]
        (is (= 1 (:hits res)))
        (is (= "job2" (get-in res [:results 0 :job/name])))))

    (let [job-id (get-in (jobs/find-all (:jobs system) "default" "job1") [:results 0 :db/id])
          end-time (.toDate (f/parse (:date f/formatters) "2016-09-09"))]
      (setup-execution (:jobs system)
                       {:db/id job-id :job-execution/end-time end-time})
      (testing "since"
        (let [res (jobs/find-all (:jobs system) "default" "since:2016-09-09")]
          (pprint res)
          (is (= 1 (:hits res)))
          (is (= "job1" (get-in res [:results 0 :job/name])))))
      (testing "until"
        (let [res (jobs/find-all (:jobs system) "default" "until:2016-09-09")]
          (is (= 1 (:hits res)))
          (is (= "job1" (get-in res [:results 0 :job/name])))))
      (testing "range"
        (let [res (jobs/find-all (:jobs system) "default" "since:2016-09-09 until:2016-09-09")]
          (is (= 1 (:hits res)))
          (is (= "job1" (get-in res [:results 0 :job/name])))))


      )))

(deftest parse-query
  (testing "parse-query"
    (let [result (jobs/parse-query "a b since:2016-09-01 until:2016-09-02 exit-status:COMPLETED batch-status:failed")]
      (is (= "a" (first (:job-name result))))
      (is (= "2016-09-01" (some->> result :since (new DateTime) (f/unparse (:date f/formatters)))))
      (is (= "2016-09-03" (some->> result :until (new DateTime) (f/unparse (:date f/formatters)))))
      (is (=  "COMPLETED" (:exit-status result)))
      (is (= :batch-status/failed (:batch-status result)))))
  (testing "exit-status is lowwer case"
    (let [result (jobs/parse-query "exit-status:completed")]
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

  (testing "ignore breaking tokens in a query"
    (let [result (jobs/parse-query "a since: until: since:xxx until:yyy")]
      (is (= {:job-name '("a")} result)))))
