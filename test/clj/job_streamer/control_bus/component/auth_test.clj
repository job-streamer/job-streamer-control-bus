(ns job-streamer.control-bus.component.auth-test
  (:require (job-streamer.control-bus.component [auth :as auth]
                                                [token :as token]
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
            [clj-time.format :as f]))

(def test-config
  {:datomic {:recreate? true
             :uri "datomic:mem://test"}
   })

(def config
  (meta-merge config/defaults
              config/environ
              test-config))

(defn new-system [config]
  (-> (component/system-map
        :apps    (apps/apps-component (:apps config))
        :auth    (auth/auth-component (:auth config))
        :token   (token/token-provider-component (:token config))
        :datomic (datomic-component   (:datomic config))
        :migration (migration-component {:dbschema model/dbschema}))
      (component/system-using
        {:auth [:token :datomic :migration :apps]
         :apps [:datomic :migration]
         :migration [:datomic]})
      (component/start-system)))

(deftest login
  (let [system (new-system config)
        handler (partial  auth/login (:auth system))]
    (testing "login success"
      (let [request {:request-method :post
                     :params {:username "admin" :password "password123" :appname "default" :next "http://localhost:3000" :back "http://localhost:3000/login"}}
            {:keys [status body session headers] :as res} (handler request)]
        (are [x y] (= x y)
             302                                        status
             "http://localhost:3000"                    (get-in headers ["Location"])
             true                                       (-> body edn/read-string :token empty? not)
             {:user/id "admin"
              :permissions #{:permission/update-job
                             :permission/read-job
                             :permission/create-job
                             :permission/delete-job
                             :permission/execute-job}} (:identity session))))
    (testing "redirect back url is forbidden"
      (let [request {:request-method :post
                     :params {:username "admin" :password "password123" :appname "default" :next "http://localhost:3000" :back "http://forbidden:3000/login"}}
            {:keys [status body session] :as res} (handler request)]
        (are [x y] (= x y)
             403                                         status
             ["Redirect to specified url is forbidden."] (-> body edn/read-string)
             nil                                         (:identity session))))
    (testing "redirect next url is forbidden"
      (let [request {:request-method :post
                     :params {:username "admin" :password "password123" :appname "default" :next "http://forbidden:3000" :back "http://localhost:3000/login"}}
            {:keys [status body session] :as res} (handler request)]
        (are [x y] (= x y)
             403                                         status
             ["Redirect to specified url is forbidden."] (-> body edn/read-string)
             nil                                         (:identity session))))
    (testing "login failure, lacking any patameter"
      (let [request {:request-method :post
                     :params {:password "password123" :appname "default" :next "http://localhost:3000" :back "http://localhost:3000/login"}}
            {:keys [status body session headers] :as res} (handler request)]
        (are [x y] (= x y)
             302                                      status
             "http://localhost:3000/login?error=true" (get-in headers ["Location"])
             ["username must be present"]             (-> body edn/read-string)
             nil                                      (:identity session))))
    (testing "login failure, authentification failure"
      (let [request {:request-method :post
                     :params {:username "admin" :password "badpassword" :appname "default" :next "http://localhost:3000" :back "http://localhost:3000/login"}}
            {:keys [status body session headers] :as res} (handler request)]
        (are [x y] (= x y)
             302                                      status
             "http://localhost:3000/login?error=true" (get-in headers ["Location"])
             ["Autification failure."]                (-> body edn/read-string)
             nil                                      (:identity session))))
    (testing "login as created user"
      (let [request {:request-method :post
                     :content-type "application/edn"
                     :body (pr-str {:user/id "addeduser" :user/password "password123" :roll "watcher"})}
            {:keys [status body]} (let [handler (auth/entry-resource (:auth system) nil)] (handler request))]
        (is (= 201 status)))
      (let [request {:request-method :post
                     :params {:username "addeduser" :password "password123" :appname "default" :next "http://localhost:3000" :back "http://localhost:3000/login"}}
            {:keys [status body session headers] :as res} (handler request)]
        (are [x y] (= x y)
             {:user/id                              "addeduser"
              :permissions #{:permission/read-job}} (:identity session))))))

(deftest logout
  (let [system (new-system config)
        handler (partial auth/logout (:auth system))]
    (testing "logout success"
      (let [request {:request-method :get
                     :identity {:user/id "admin"}
                     :params {:next "http://localhost:3000/login"}}
            {:keys [status session headers] :as res} (handler request)]
        (are [x y] (= x y)
             302                           status
             "http://localhost:3000/login" (get-in headers ["Location"])
             nil                           (:identity session))))))

(deftest list-resource
  (let [system (new-system config)
        handler (auth/list-resource (:auth system))]
    (testing "get only default user"
      (let [request {:request-method :get}
            {:keys [status body]} (handler request)]
        (are [x y] (= x y)
             200     status
             1       (-> body edn/read-string count)
             "admin" (-> body edn/read-string first :user/id))))))

(deftest entry-resource
  ;; create user cases.
  (let [system (new-system config)
        handler (auth/entry-resource (:auth system) nil)]
    (testing "lacking id"
      (let [request {:request-method :post
                     :content-type "application/edn"
                     :body (pr-str {:user/password "password123" :roll "watcher"})}
            {:keys [status body]} (handler request)]
        (are [x y] (= x y)
             400 status
             ["id must be present"] (-> body edn/read-string :messages))))
    (testing "lacking password"
      (let [request {:request-method :post
                     :content-type "application/edn"
                     :body (pr-str {:user/id "test" :roll "watcher"})}
            {:keys [status body]} (handler request)]
        (are [x y] (= x y)
             400 status
             ["password must be present" "token must be present"] (-> body edn/read-string :messages))))
    (testing "lacking roll"
      (let [request {:request-method :post
                     :content-type "application/edn"
                     :body (pr-str {:user/id "test" :user/password "password123"})}
            {:keys [status body]} (handler request)]
        (are [x y] (= x y)
             400 status
             ["roll must be present"] (-> body edn/read-string :messages))))
    (testing "id does not satisfy min length"
      (let [request {:request-method :post
                     :content-type "application/edn"
                     :body (pr-str {:user/id "te" :user/password "password123" :roll "watcher"})}
            {:keys [status body]} (handler request)]
        (are [x y] (= x y)
             400 status
             ["Username must be at least 3 characters long."] (-> body edn/read-string :messages))))
    (testing "id does not satisfy max length"
      (let [request {:request-method :post
                     :content-type "application/edn"
                     :body (pr-str {:user/id "test12345678901234567" :user/password "password123" :roll "watcher"})}
            {:keys [status body]} (handler request)]
        (are [x y] (= x y)
             400 status
             ["Username is too long."] (-> body edn/read-string :messages))))
    (testing "password does not satisfy min length"
      (let [request {:request-method :post
                     :content-type "application/edn"
                     :body (pr-str {:user/id "test" :user/password "passwor" :roll "watcher"})}
            {:keys [status body]} (handler request)]
        (are [x y] (= x y)
             400 status
             ["Password must be at least 8 characters long."] (-> body edn/read-string :messages))))
    (testing "id conflicts"
      (let [request {:request-method :post
                     :content-type "application/edn"
                     :body (pr-str {:user/id "admin" :user/password "password123" :roll "watcher"})}
            {:keys [status body]} (handler request)]
        (are [x y] (= x y)
             400 status
             ["id is used by someone."] (-> body edn/read-string :messages))))
    (testing "invalid roll"
      (let [request {:request-method :post
                     :content-type "application/edn"
                     :body (pr-str {:user/id "test" :user/password "password123" :roll "nothing"})}
            {:keys [status body]} (handler request)]
        (are [x y] (= x y)
             400 status
             ["roll is used by someone."] (-> body edn/read-string :messages))))
    (testing "create user"
      (let [request {:request-method :post
                     :content-type "application/edn"
                     :body (pr-str {:user/id "test" :user/password "password123" :roll "watcher"})}
            {:keys [status body]} (handler request)]
        (is (= 201 status)))
      (let [handler (auth/list-resource (:auth system))
            request {:request-method :get}
            {:keys [status body]} (handler request)]
        (are [x y] (= x y)
             200     status
             2       (-> body edn/read-string count))))
    (let [system (new-system config)
          handler (auth/entry-resource (:auth system) "test")]
      (testing "delete user"
        (let [request {:request-method :post
                       :content-type "application/edn"
                       :body (pr-str {:user/id "test" :user/password "password123" :roll "watcher"})}
              {:keys [status body]} (handler request)]
          (is (= 201 status)))
        (let [request {:request-method :delete
                       :content-type "application/edn"
                       :body (pr-str {:user/password "password123" :roll "watcher"})}
              {:keys [status body]} (handler request)]
          (is (== 204 status)))
        (let [handler (auth/list-resource (:auth system))
              request {:request-method :get}
              {:keys [status body]} (handler request)]
          (are [x y] (= x y)
               200     status
               1       (-> body edn/read-string count)))))))
