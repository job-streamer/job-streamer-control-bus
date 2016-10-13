(ns job-streamer.control-bus.component.apps-test
  (:require (job-streamer.control-bus.component [apps :as apps]
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
            [clj-time.format :as f]
            [clojure.java.io :as io]))

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
        :datomic (datomic-component   (:datomic config))
        :migration (migration-component {:dbschema model/dbschema}))
      (component/system-using
        {:apps [:datomic]
         :migration [:datomic]})
      (component/start-system)))

(deftest batch-components-resource
  (let [system (new-system config)
        handler (-> (apps/batch-components-resource (:apps system) "default"))]

    (testing "upload a jar"
      (let [request {:request-method :post
                     :content-type "application/octet-stream"
                     :params {"file" {:filename "test.jar" :tempfile (-> "test.tmp" io/resource io/file) :size 13}}}
            uploaded (io/file "batch-components/default/test.jar")
            delete-updated #(when (.exists uploaded)
                              (io/delete-file uploaded))]
        (try
          (delete-updated)
          (is (is (= 201 (-> (handler request) :status))))
          (is (.exists uploaded))
          (finally (delete-updated)))))

    (testing "bad file extension"
      (let [request {:request-method :post
                     :content-type "application/octet-stream"
                     :params {"file" {:filename "test.jpg" :tempfile (-> "test.tmp" io/resource io/file) :size 13}}}]
        (is (is (= 400 (-> (handler request) :status))))))))
