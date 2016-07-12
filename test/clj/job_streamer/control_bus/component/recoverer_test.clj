(ns job-streamer.control-bus.component.recoverer-test
  (:require (job-streamer.control-bus.component [recoverer :refer :all]
                                                [datomic :refer [IDataSource] :as d])
            [com.stuartsierra.component :as component]
            [clojure.core.async :refer [go <!! timeout]]
            [clojure.test :refer :all]
            [clojure.pprint :refer :all]
            [shrubbery.core :refer :all]))

(defn datomic-mock []
  (spy (reify IDataSource
         (query*   [this q params])
         (pull     [this pattern eid])
         (transact [this transaction])
         (resolve-tempid [this tempids tempid]))))

(deftest recoverer-test
  (testing "start-component"
    (let [datomic (datomic-mock)
          recoverer (component/start (recoverer-component
                                      {:datomic datomic
                                       :initial-interval 100}))]
      (do (<!! (timeout 150))
        (is (received? datomic d/query*))
        (component/stop recoverer)))))
