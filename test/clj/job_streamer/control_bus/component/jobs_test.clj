(ns job-streamer.control-bus.component.jobs-test
  (:require (job-streamer.control-bus.component [jobs :as jobs]
                                                [apps :as apps]
                                                [scheduler :as scheduler]
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
       :scheduler (scheduler/scheduler-component (:scheduler config))
       :datomic (datomic-component   (:datomic config))
       :migration (migration-component {:dbschema model/dbschema}))
      (component/system-using
       {:jobs [:datomic :migration :scheduler]
        :apps [:datomic]
        :scheduler [:datomic]
        :migration [:datomic]})
      (component/start-system)))

(defn setup-execution [{:keys [datomic] :as jobs}
                       {:keys [job-execution/start-time db/id job-execution/end-time
                               job-execution/create-time job-execution/batch-status
                               job-execution/exit-status]
                        :or {job-execution/start-time (java.util.Date.)
                             job-execution/end-time (java.util.Date.)
                             job-execution/create-time (java.util.Date.)
                             job-execution/batch-status :batch-status/undispatched
                             job-execution/exit-status "COMPLETED"}}]
  (let [execution-id (d/tempid :db.part/user)]
    (-> (d/transact
         datomic
         [{:db/id execution-id
           :job-execution/batch-status batch-status
           :job-execution/create-time create-time
           :job-execution/end-time end-time
           :job-execution/start-time start-time
           :job-execution/exit-status exit-status
           :job-execution/job-parameters "{}"}
          [:db/add id :job/executions execution-id]])
        :tempids)))

(def all-permissions #{:permission/update-job :permission/read-job :permission/create-job :permission/delete-job :permission/execute-job})

(deftest find-all
  (testing "find-all"
    (let [system (new-system config)]
      (let [res (jobs/find-all (:jobs system) "default" nil)]
        (is (= 0 (count res)))
        (is (empty? res))))))

(deftest list-resource
  (let [system (new-system config)
        handler (-> (jobs/list-resource (:jobs system) "default"))]
    (testing "no jobs"
      (let [request {:request-method :get :identity {:permissions all-permissions}}]
        (is (= 0 (-> (handler request) :body edn/read-string :hits)))))
    (testing "validation error"
      (let [request {:request-method :post
                     :identity {:permissions all-permissions}
                     :content-type "application/edn"
                     :body (pr-str {})}]
        (is (= ["name must be present"]
               (-> (handler request) :body edn/read-string :messages)))))
    (testing "create a job"
      (let [request {:request-method :post
                     :identity {:permissions all-permissions}
                     :content-type "application/edn"
                     :body (pr-str {:job/name "job1"})}]
        (is (= 201 (-> (handler request) :status))))
      (let [res (jobs/find-all (:jobs system) "default" nil)]
        (is (= 1 (count res)))))
    (testing "create a job with the schedule and the status-notifications."
      (let [request {:request-method :post
                     :identity {:permissions all-permissions}
                     :content-type "application/edn"
                     :body (pr-str {:job/name "test"
                                    :job/schedule {:db/id 17592186045822
                                                   :schedule/cron-notation "0 0 12 * * ?"
                                                   :schedule/active? true}
                                    :job/edn-notation "{:job/status-notifications [{:status-notification/type \"test-notification\"
                                                                                    :status-notification/batch-status {:db/ident :batch-status/abandoned}}]
                                                        :job/schedule nil
                                                        :job/name \"test\"
                                                        :job/components [{:step/name \"test-step\"
                                                                          :step/properties nil
                                                                          :step/batchlet {:batchlet/ref \"org.jobstreamer.batch.ShellBatchlet\"}}]
                                                        :job/exclusive? false
                                                        :job/properties nil}"
                                    :job/exclusive? false
                                    :job/status-notifications [{:status-notification/batch-status {:db/ident :batch-status/abandoned}
                                                                :status-notification/type "test-notification"}]})}]
        (is (= 201 (-> (handler request) :status))))
      (let [handler (-> (jobs/entry-resource (:jobs system) "default" "test"))
            request {:request-method :get
                     :identity {:permissions #{:permission/read-job :permission/update-job :permission/create-job :permission/delete-job :permission/execute-job}}
                     :content-type "application/edn"}]
        (let [{:keys [status body]} (handler request)
              {:keys [job/schedule job/status-notifications]} (read-string body)]
          (are [x y] (= x y)
               status 200
               (count status-notifications) 1
               (nil? schedule) false))))
    (testing "read a job is not authorized"
      (let [request {:request-method :get
                     :identity {:permissions #{:permission/update-job :permission/create-job :permission/delete-job :permission/execute-job}}
                     :content-type "application/edn"
                     :body (pr-str {:job/name "job1"})}]
        (is (= 403 (-> (handler request) :status)))))
    (testing "create a job is not authorized"
      (let [request {:request-method :post
                     :identity {:permissions #{:permission/update-job :permission/read-job :permission/delete-job :permission/execute-job}}
                     :content-type "application/edn"
                     :body (pr-str {:job/name "job1"})}]
        (is (= 403 (-> (handler request) :status)))))))

(deftest download-list-resource
  (let [system (new-system config)
        handler (-> (jobs/list-resource (:jobs system) "default" :download? true))]
    (testing "no jobs"
      (let [request {:request-method :get :identity {:permissions all-permissions}}]
        (is (empty? (-> (handler request) :body read-string)))))
    (testing "has a job"
      (let [request {:request-method :post
                     :identity {:permissions all-permissions}
                     :content-type "application/edn"
                     :body (pr-str {:job/name "job1"})}]
        (is (= 201 (-> ((-> (jobs/list-resource (:jobs system) "default")) request) :status))))
      (let [request {:request-method :get :identity {:permissions all-permissions}}
            response (handler request)]
        (is (= "job1" (-> response :body read-string first :job/name)))
        (is (= "application/force-download"  ((:headers response) "Content-Type")))
        (is (= "attachment; filename=\"jobs.edn\""  ((:headers response) "Content-disposition")))))
    (testing "download job is not authorized"
      (let [request {:request-method :get :identity {:permissions #{:permission/update-job :permission/create-job :permission/delete-job :permission/execute-job}}}
            response (handler request)]
        (is (= 403 (-> (handler request) :status)))))))

(deftest find-all-with-query
  (let [system (new-system config)
        handler (-> (jobs/list-resource (:jobs system) "default"))]
    ;; setup data
    (handler {:request-method :post
              :identity {:permissions all-permissions}
              :content-type "application/edn"
              :body (pr-str {:job/name "job1"})})
    (handler {:request-method :post
              :identity {:permissions all-permissions}
              :content-type "application/edn"
              :body (pr-str {:job/name "job2"})})
    (testing "Mathes exactly"
      (let [res (jobs/find-all (:jobs system) "default" "job1")]
        (is (= 1 (count res)))))

    (testing "A query of multiple keywords"
      (let [res (jobs/find-all (:jobs system) "default" "job1 job2")]
        (is (= 2 (count res)))))

    (testing "backward matching"
      (let [res (jobs/find-all (:jobs system) "default" "b2")]
        (is (= 1 (count res)))
        (is (= "job2" (->> res first :job/name)))))

    (let [job-id (-> (jobs/find-all (:jobs system) "default" "job1")
                     first
                     :db/id)
          create-time (.toDate (f/parse (:date f/formatters) "2016-09-01"))
          start-time (.toDate (f/parse (:date f/formatters) "2016-09-09"))
          end-time (.toDate (f/parse (:date f/formatters) "2016-09-10"))]
      (setup-execution (:jobs system)
                       {:db/id job-id
                        :job-execution/end-time end-time
                        :job-execution/start-time start-time
                        :job-execution/create-time create-time})
      (testing "since"
        (let [res (jobs/find-all (:jobs system) "default" "since:2016-09-09")]
          (is (= 1 (count res)))
          (is (= "job1" (->> res first :job/name)))))
      (testing "until"
        (let [res (jobs/find-all (:jobs system) "default" "until:2016-09-10")]
          (is (= 1 (count res)))
          (is (= "job1" (->> res first :job/name)))))
      (testing "range"
        (let [res (jobs/find-all (:jobs system) "default" "since:2016-09-09 until:2016-09-10")]
          (is (= 1 (count res)))
          (is (= "job1"  (->> res first :job/name)))))
      (testing "exit-status"
        (let [res (jobs/find-all (:jobs system) "default" "exit-status:COMPLETED")]
          (is (= 1 (count res)))
          (is (= "job1"  (->> res first :job/name)))))
      (testing "batch-status"
        (let [res (jobs/find-all (:jobs system) "default" "batch-status:undispatched")]
          (is (= 1 (count res)))
          (is (= "job1" (->> res first :job/name)))))
      ;; Setup one more execution.
      (let [create-time (.toDate (f/parse (:date f/formatters) "2016-09-11"))
            start-time (.toDate (f/parse (:date f/formatters) "2016-09-12"))
            end-time (.toDate (f/parse (:date f/formatters) "2016-09-13"))
            exit-status "FAILED"]
        (setup-execution (:jobs system)
                         {:db/id job-id
                          :job-execution/end-time end-time
                          :job-execution/start-time start-time
                          :job-execution/create-time create-time
                          :job-execution/exit-status exit-status})
        (testing "exit-status: search only latest execution and not found"
          (let [res (jobs/find-all (:jobs system) "default" "exit-status:COMPLETED")]
            (is (= 0 (count res)))))
        (testing "exit-status: search only latest execution and found"
          (let [res (jobs/find-all (:jobs system) "default" "exit-status:FAILED")]
            (is (= 1 (count res)))
            (is (= "job1"  (->> res first :job/name)))))))))

(deftest find-all-with-sort
  (let [system (new-system config)
        handler (-> (jobs/list-resource (:jobs system) "default"))]
    ;; setup data
    (handler {:request-method :post
              :identity {:permissions all-permissions}
              :content-type "application/edn"
              :body (pr-str {:job/name "job1"})})
    (handler {:request-method :post
              :identity {:permissions all-permissions}
              :content-type "application/edn"
              :body (pr-str {:job/name "job2"})})
    (handler {:request-method :post
              :identity {:permissions all-permissions}
              :content-type "application/edn"
              :body (pr-str {:job/name "job3"})})
    (testing "sort-by-name"
      (let [res (->> (jobs/find-all (:jobs system) "default" "")
                     (jobs/sort-by-map [[:name :asc]]))]
        (is (= 3 (count res)))
        (is (= (list "job1" "job2" "job3")
               (map :job/name res))))

      (let [res (->> (jobs/find-all (:jobs system) "default" "")
                     (jobs/sort-by-map [[:name :desc]]))]
        (is (= 3 (count res)))
        (is (= (list "job3" "job2" "job1")
               (map :job/name res)))))

    (let [job-id (->> (jobs/find-all (:jobs system) "default" "job1")
                      first
                      :db/id)
          create-time (.toDate (f/parse (:date f/formatters) "2016-09-01"))
          start-time (.toDate (f/parse (:date f/formatters) "2016-09-09"))
          end-time (.toDate (f/parse (:date f/formatters) "2016-09-10"))
          batch-status :batch-status/stopping]
      (setup-execution (:jobs system)
                       {:db/id job-id
                        :job-execution/end-time end-time
                        :job-execution/start-time start-time
                        :job-execution/create-time create-time
                        :job-execution/batch-status batch-status}))
    (let [job-id (->> (jobs/find-all (:jobs system) "default" "job2")
                      first
                      :db/id)
          create-time (.toDate (f/parse (:date f/formatters) "2016-09-01"))
          start-time (.toDate (f/parse (:date f/formatters) "2016-09-10"))
          end-time (.toDate (f/parse (:date f/formatters) "2016-09-13"))
          batch-status :batch-status/failed]

      (setup-execution (:jobs system)
                       {:db/id job-id
                        :job-execution/end-time end-time
                        :job-execution/start-time start-time
                        :job-execution/create-time create-time
                        :job-execution/batch-status batch-status}))
    (let [job-id (->> (jobs/find-all (:jobs system) "default" "job3")
                      first
                      :db/id)
          create-time (.toDate (f/parse (:date f/formatters) "2016-09-01"))
          start-time (.toDate (f/parse (:date f/formatters) "2016-09-8"))
          end-time (.toDate (f/parse (:date f/formatters) "2016-09-10"))
          batch-status :batch-status/completed]
      (setup-execution (:jobs system)
                       {:db/id job-id
                        :job-execution/end-time end-time
                        :job-execution/start-time start-time
                        :job-execution/create-time create-time
                        :job-execution/batch-status batch-status}))
    (testing "sort-by-last-execution-start"
      (let [res (->> (jobs/find-all (:jobs system) "default" "")
                     (jobs/include-job-attrs (:jobs system) #{:execution})
                     (jobs/sort-by-map [[:last-execution-started :asc]]))]
        (is (= 3 (count res)))
        (is (= (list "job3" "job1" "job2")
               (map :job/name res))))
      (let [res (->> (jobs/find-all (:jobs system) "default" "")
                     (jobs/include-job-attrs (:jobs system) #{:execution})
                     (jobs/sort-by-map [[:last-execution-started :desc]]))]
        (is (= 3 (count res)))
        (is (= (list "job2" "job1" "job3")
               (map :job/name res)))))
    (testing "sort-by-last-execution-duration"
      (let [res (->> (jobs/find-all (:jobs system) "default" "")
                     (jobs/include-job-attrs (:jobs system) #{:execution})
                     (jobs/sort-by-map [[:last-execution-duration :asc]]))]
        (is (= 3 (count res)))
        (is (= (list "job1" "job3" "job2")
               (map :job/name res))))
      (let [res (->> (jobs/find-all (:jobs system) "default" "")
                     (jobs/include-job-attrs (:jobs system) #{:execution})
                     (jobs/sort-by-map [[:last-execution-duration :desc]]))]
        (is (= 3 (count res)))
        (is (= (list "job2" "job3" "job1")
               (map :job/name res)))))
    (testing "sort-by-last-execution-status"
      (let [res (->> (jobs/find-all (:jobs system) "default" "")
                     (jobs/include-job-attrs (:jobs system) #{:execution})
                     (jobs/sort-by-map [[:last-execution-status :asc]]))]
        (is (= 3 (count res)))
        (is (= (list "job3" "job2" "job1")
               (map :job/name res))))
      (let [res (->> (jobs/find-all (:jobs system) "default" "")
                     (jobs/include-job-attrs (:jobs system) #{:execution})
                     (jobs/sort-by-map [[:last-execution-status :desc]]))]
        (is (= 3 (count res)))
        (is (= (list "job1" "job2" "job3")
               (map :job/name res)))))
    (testing "sort-by-next-execution-start"
      (let [res (jobs/sort-by-map
                 {:next-execution-start :asc}
                 (read-string "({:job/name \"job1\", :job/executions ({:db/id 17592186045449, :job-execution/create-time #inst \"2016-09-01T00:00:00.000-00:00\", :job-execution/start-time #inst \"2016-09-09T00:00:00.000-00:00\", :job-execution/end-time #inst \"2016-09-10T00:00:00.000-00:00\", :job-execution/exit-status COMPLETED, :job-execution/batch-status {:db/ident :batch-status/stopping}}), :job/latest-execution {:db/id 17592186045449, :job-execution/create-time #inst \"2016-09-01T00:00:00.000-00:00\", :job-execution/start-time #inst \"2016-09-09T00:00:00.000-00:00\", :job-execution/end-time #inst \"2016-09-10T00:00:00.000-00:00\", :job-execution/exit-status COMPLETED, :job-execution/batch-status {:db/ident :batch-status/stopping}}, :job/next-execution {:job-execution/start-time #inst \"2016-09-08T00:00:00.000-00:00\"}} {:job/name \"job2\", :job/executions ({:db/id 17592186045451, :job-execution/create-time #inst \"2016-09-01T00:00:00.000-00:00\", :job-execution/start-time #inst \"2016-09-10T00:00:00.000-00:00\", :job-execution/end-time #inst \"2016-09-13T00:00:00.000-00:00\", :job-execution/exit-status COMPLETED, :job-execution/batch-status {:db/ident :batch-status/failed}}), :job/latest-execution {:db/id 17592186045451, :job-execution/create-time #inst \"2016-09-01T00:00:00.000-00:00\", :job-execution/start-time #inst \"2016-09-10T00:00:00.000-00:00\", :job-execution/end-time #inst \"2016-09-13T00:00:00.000-00:00\", :job-execution/exit-status COMPLETED, :job-execution/batch-status {:db/ident :batch-status/failed}}, :job/next-execution {:job-execution/start-time #inst \"2016-09-07T00:00:00.000-00:00\"}} {:job/name \"job3\", :job/executions ({:db/id 17592186045453, :job-execution/create-time #inst \"2016-09-01T00:00:00.000-00:00\", :job-execution/start-time #inst \"2016-09-08T00:00:00.000-00:00\", :job-execution/end-time #inst \"2016-09-10T00:00:00.000-00:00\", :job-execution/exit-status COMPLETED, :job-execution/batch-status {:db/ident :batch-status/completed}}), :job/latest-execution {:db/id 17592186045453, :job-execution/create-time #inst \"2016-09-01T00:00:00.000-00:00\", :job-execution/start-time #inst \"2016-09-08T00:00:00.000-00:00\", :job-execution/end-time #inst \"2016-09-10T00:00:00.000-00:00\", :job-execution/exit-status COMPLETED, :job-execution/batch-status {:db/ident :batch-status/completed}}, :job/next-execution {:job-execution/start-time #inst \"2016-09-09T00:00:00.000-00:00\"}})"))]
        (is (= "job2" (-> res first :job/name)))
        (is (= "job1" (-> res second :job/name)))
        (is (= "job3" (-> res (nth 2) :job/name))))
      (let [res (jobs/sort-by-map
                  {:next-execution-start :desc}
                  (read-string "({:job/name \"job1\", :job/executions ({:db/id 17592186045449, :job-execution/create-time #inst \"2016-09-01T00:00:00.000-00:00\", :job-execution/start-time #inst \"2016-09-09T00:00:00.000-00:00\", :job-execution/end-time #inst \"2016-09-10T00:00:00.000-00:00\", :job-execution/exit-status COMPLETED, :job-execution/batch-status {:db/ident :batch-status/stopping}}), :job/latest-execution {:db/id 17592186045449, :job-execution/create-time #inst \"2016-09-01T00:00:00.000-00:00\", :job-execution/start-time #inst \"2016-09-09T00:00:00.000-00:00\", :job-execution/end-time #inst \"2016-09-10T00:00:00.000-00:00\", :job-execution/exit-status COMPLETED, :job-execution/batch-status {:db/ident :batch-status/stopping}}, :job/next-execution {:job-execution/start-time #inst \"2016-09-08T00:00:00.000-00:00\"}} {:job/name \"job2\", :job/executions ({:db/id 17592186045451, :job-execution/create-time #inst \"2016-09-01T00:00:00.000-00:00\", :job-execution/start-time #inst \"2016-09-10T00:00:00.000-00:00\", :job-execution/end-time #inst \"2016-09-13T00:00:00.000-00:00\", :job-execution/exit-status COMPLETED, :job-execution/batch-status {:db/ident :batch-status/failed}}), :job/latest-execution {:db/id 17592186045451, :job-execution/create-time #inst \"2016-09-01T00:00:00.000-00:00\", :job-execution/start-time #inst \"2016-09-10T00:00:00.000-00:00\", :job-execution/end-time #inst \"2016-09-13T00:00:00.000-00:00\", :job-execution/exit-status COMPLETED, :job-execution/batch-status {:db/ident :batch-status/failed}}, :job/next-execution {:job-execution/start-time #inst \"2016-09-07T00:00:00.000-00:00\"}} {:job/name \"job3\", :job/executions ({:db/id 17592186045453, :job-execution/create-time #inst \"2016-09-01T00:00:00.000-00:00\", :job-execution/start-time #inst \"2016-09-08T00:00:00.000-00:00\", :job-execution/end-time #inst \"2016-09-10T00:00:00.000-00:00\", :job-execution/exit-status COMPLETED, :job-execution/batch-status {:db/ident :batch-status/completed}}), :job/latest-execution {:db/id 17592186045453, :job-execution/create-time #inst \"2016-09-01T00:00:00.000-00:00\", :job-execution/start-time #inst \"2016-09-08T00:00:00.000-00:00\", :job-execution/end-time #inst \"2016-09-10T00:00:00.000-00:00\", :job-execution/exit-status COMPLETED, :job-execution/batch-status {:db/ident :batch-status/completed}}, :job/next-execution {:job-execution/start-time #inst \"2016-09-09T00:00:00.000-00:00\"}})"))]
        (is (= "job2" (-> res (nth 2) :job/name)))
        (is (= "job1" (-> res second :job/name)))
        (is (= "job3" (-> res first :job/name)))))
    (testing "multiple-query"
      (let [res (->> (jobs/find-all (:jobs system) "default" "")
                     (jobs/include-job-attrs (:job system) #{:execution})
                     (jobs/sort-by-map [[:name :desc]
                                        [:last-execution-status :asc]]))]
        (is (= (list "job3" "job2" "job1")
               (map :job/name res))))
      (let [res (->> (jobs/find-all (:jobs system) "default" "")
                     (jobs/include-job-attrs (:job system) #{:execution})
                     (jobs/sort-by-map [[:last-execution-status :desc]
                                        [:name :desc]]))]
        (is (= 3 (count res)))
        (is (= (list "job1" "job2" "job3")
               (map :job/name res)))))
    (handler {:request-method :post
              :identity {:permissions all-permissions}
              :content-type "application/edn"
              :body (pr-str {:job/name "job4"})})
    (let [job-id (->> (jobs/find-all (:jobs system) "default" "job4")
                      first
                      :db/id)
          create-time (.toDate (f/parse (:date f/formatters) "2016-09-01"))
          start-time (.toDate (f/parse (:date f/formatters) "2016-09-8"))
          end-time (.toDate (f/parse (:date f/formatters) "2016-09-10"))
          batch-status :batch-status/completed]
      (setup-execution (:jobs system)
                       {:db/id job-id
                        :job-execution/end-time end-time
                        :job-execution/start-time start-time
                        :job-execution/create-time create-time
                        :job-execution/batch-status batch-status}))
    (testing "multiple-query-sort-by-second-key"
      (let [res (->> (jobs/find-all (:jobs system) "default" "")
                     (jobs/include-job-attrs (:jobs system) #{:execution})
                     (jobs/sort-by-map [[:last-execution-status :asc]
                                   [:name :asc]])) ]
        (is (= 4 (count res)))
        (is (= (list "job3" "job4" "job2" "job1")
               (map :job/name res))))
      (let [res (->> (jobs/find-all (:jobs system) "default" "")
                     (jobs/include-job-attrs (:job system) #{:execution})
                     (jobs/sort-by-map [[:last-execution-status :asc]
                                        [:name :desc]]))]
        (is (= 4 (count res)))
        (is (= (list "job4" "job3" "job2" "job1")
               (map :job/name res)))))))

(deftest save-execution
  (let [system (new-system config)]
    ;; setup data
    ((-> (jobs/list-resource (:jobs system) "default")) {:request-method :post
              :identity {:permissions all-permissions}
              :content-type "application/edn"
              :body (pr-str {:job/name "job1"})})

    (testing "save step-executions"
      (let [job-id (->> (jobs/find-all (:jobs system) "default" "job1")
                        first
                        :db/id)
            create-time (.toDate (f/parse (:date f/formatters) "2016-09-01"))
            start-time (.toDate (f/parse (:date f/formatters) "2016-09-09"))
            end-time (.toDate (f/parse (:date f/formatters) "2016-09-10"))
            batch-status :batch-status/stopping
            execution-id (-> (setup-execution (:jobs system)
                                          {:db/id job-id
                                           :job-execution/end-time end-time
                                           :job-execution/start-time start-time
                                           :job-execution/create-time create-time
                                           :job-execution/batch-status batch-status})
                             vals
                             first)
            handler (-> (jobs/execution-resource (:jobs system) execution-id))
            request {:request-method :get :identity {:permissions all-permissions}}]
        (jobs/save-execution (:jobs system) execution-id
                       {:batch-status :batch-status/completed :exit-status "SUPERSUCCESS" :step-executions
                        [{:end-time end-time
                          :start-time start-time
                          :batch-status batch-status
                          :exit-status "SUCCESS"
                          :step-execution-id 11111111111111}]})
        (is (= "SUCCESS" (-> request handler :body read-string :job-execution/step-executions first :step-execution/exit-status)))))))

(deftest executions-resource
  (let [system (new-system config)]
    ;; setup data
    ((-> (jobs/list-resource (:jobs system) "default")) {:request-method :post
              :content-type "application/edn"
              :body (pr-str {:job/name "job1"})})
    (testing "return 404 when job-name is not exist"
      (let [handler (-> (jobs/executions-resource (:jobs system) "default" "job2"))
            request {:request-method :post}]
        (is (= 404 (-> request handler :status)))))))

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

(deftest parse-sort-order
  (testing "parse-query-nomal"
    (let [result (jobs/parse-sort-order "name:asc,last-execution-status:desc")]
      (is (= :asc (:name result)))
      (is (= :desc (:last-execution-status result)))))
  (testing "parse-query-invalid-name"
    (let [result (jobs/parse-sort-order "name:asc,last-execution-status:desc,something:asc")]
      (is (= :asc (:name result)))
      (is (= :desc (:last-execution-status result)))
      (is (not= :asc (:something result)))))
  (testing "parse-query-invalid-sort-order"
    (let [result (jobs/parse-sort-order "name:asc,last-execution-status:desc,next-execution-start:random")]
      (is (= :asc (:name result)))
      (is (= :desc (:last-execution-status result)))
      (is (not= :random (:next-execution-start result)))))
  (testing "parse-query-sort-order's-order"
    (let [result (jobs/parse-sort-order "last-execution-status:desc,name:asc,next-execution-start:asc,last-execution-started:desc,last-execution-duration:asc")]
      (is (= :last-execution-status (-> result first first)))
      (is (= :desc (-> result first second)))
      (is (= :name (-> result second first)))
      (is (= :asc (-> result second second)))
      (is (= :next-execution-start (-> result seq (nth 2) first)))
      (is (= :asc (-> result seq (nth 2) second)))
      (is (= :last-execution-started (-> result seq (nth 3) first)))
      (is (= :desc (-> result seq (nth 3) second)))
      (is (= :last-execution-duration (-> result seq (nth 4) first)))
      (is (= :asc (-> result seq (nth 4) second))))))

(deftest entry-resource
  (let [system (new-system config)
        handler (-> (jobs/entry-resource (:jobs system) "default" "test-job"))]
    (testing "read entry is not authorized"
      (let [request {:request-method :get
                     :identity {:permissions #{:permission/update-job :permission/create-job :permission/delete-job :permission/execute-job}}
                     :content-type "application/edn"}]
        (is (= 403 (-> (handler request) :status)))))
    (testing "create entry is not authorized"
      (let [request {:request-method :put
                     :identity {:permissions #{:permission/read-job :permission/create-job :permission/delete-job :permission/execute-job}}
                     :content-type "application/edn"}]
        (is (= 403 (-> (handler request) :status)))))
    (testing "delete entry is not authorized"
      (let [request {:request-method :delete
                     :identity {:permissions #{:permission/read-job :permission/update-job :permission/create-job :permission/execute-job}}
                     :content-type "application/edn"}]
        (is (= 403 (-> (handler request) :status)))))))

(deftest job-settings-resource
  (let [system (new-system config)
        handler (-> (jobs/job-settings-resource (:jobs system) "default" "test-job"))]
    (testing "read setting is not authorized"
      (let [request {:request-method :get
                     :identity {:permissions #{:permission/update-job :permission/create-job :permission/delete-job :permission/execute-job}}
                     :content-type "application/edn"}]
        (is (= 403 (-> (handler request) :status)))))
    (testing "update setting is not authorized"
      (let [request {:request-method :put
                     :identity {:permissions #{:permission/read-job :permission/create-job :permission/delete-job :permission/execute-job}}
                     :content-type "application/edn"}]
        (is (= 403 (-> (handler request) :status)))))
    (testing "delete setting is not authorized"
      (let [request {:request-method :delete
                     :identity {:permissions #{:permission/read-job :permission/update-job :permission/create-job :permission/execute-job}}
                     :content-type "application/edn"}]
        (is (= 403 (-> (handler request) :status)))))))

(deftest executions-resource
  (let [system (new-system config)
        handler (-> (jobs/executions-resource (:jobs system) "default" "test-job"))]
    (testing "read executions is not authorized"
      (let [request {:request-method :get
                     :identity {:permissions #{:permission/update-job :permission/create-job :permission/delete-job :permission/execute-job}}
                     :content-type "application/edn"}]
        (is (= 403 (-> (handler request) :status)))))
    (testing "executions are not authorized"
      (let [request {:request-method :post
                     :identity {:permissions #{:permission/read-job :permission/update-job :permission/create-job :permission/delete-job}}
                     :content-type "application/edn"}]
        (is (= 403 (-> (handler request) :status)))))))

(deftest execution-resource
  (let [system (new-system config)
        handler (-> (jobs/execution-resource (:jobs system) "default" "test-job"))]
    (testing "read execution is not authorized"
      (let [request {:request-method :get
                     :identity {:permissions #{:permission/update-job :permission/create-job :permission/delete-job :permission/execute-job}}
                     :content-type "application/edn"}]
        (is (= 403 (-> (handler request) :status)))))
    (testing "execution is not authorized"
      (let [request {:request-method :put
                     :identity {:permissions #{:permission/read-job :permission/update-job :permission/create-job :permission/delete-job}}
                     :content-type "application/edn"}]
        (is (= 403 (-> (handler request) :status)))))))

(deftest extract-job-parameters
    (testing "has dynamic param"
      (is (= (jobs/extract-job-parameters
 {:job/bpmn-xml-notation "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<bpmn:definitions xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:bpmn=\"http://www.omg.org/spec/BPMN/20100524/MODEL\" xmlns:jsr352=\"http://jsr352/\" xmlns:bpmndi=\"http://www.omg.org/spec/BPMN/20100524/DI\" xmlns:dc=\"http://www.omg.org/spec/DD/20100524/DC\" xmlns:di=\"http://www.omg.org/spec/DD/20100524/DI\" xmlns:camunda=\"http://camunda.org/schema/1.0/bpmn\" id=\"Definitions_1\" targetNamespace=\"http://bpmn.io/schema/bpmn\">
 <jsr352:job id=\"Job_1\" isExecutable=\"false\">
   <bpmn:extensionElements>
     <camunda:properties>
       <camunda:property name=\"k\" value=\"#{jobParameters['job-param']}\" />
     </camunda:properties>
   </bpmn:extensionElements>"}) #{"job-param"}))))

