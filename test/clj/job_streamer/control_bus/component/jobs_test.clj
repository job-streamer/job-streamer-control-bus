(ns job-streamer.control-bus.component.jobs-test
  (:require (job-streamer.control-bus.component [jobs :as jobs]
                                                [datomic :refer [datomic-component]]
                                                [migration :refer [migration-component]])
            (job-streamer.control-bus [system :as system]
                                      [model :as model]
                                      [config :as config])
            [com.stuartsierra.component :as component]
            [meta-merge.core :refer [meta-merge]]
            [clojure.test :refer :all]
            [kerodon.core :refer :all]
            [kerodon.test :refer :all]))

(deftest find-all
  (testing "find-all"
    (let [system (-> (component/system-map
                      :jobs    (jobs/jobs-component {})
                      :datomic (datomic-component {:uri "datomic:mem://test"})
                      :migration (migration-component {:dbschema model/dbschema}))
                     (component/system-using
                      {:jobs [:datomic :migration]
                       :migration [:datomic]})
                     (component/start-system))]
      (let [res (jobs/find-all (:jobs system) "default" nil)]
        (is (= 0 (:hits res)))
        (is (empty? (:results res)))))))
