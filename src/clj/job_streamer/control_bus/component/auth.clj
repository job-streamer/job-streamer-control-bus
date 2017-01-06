(ns job-streamer.control-bus.component.auth
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [buddy.core.nonce :as nonce]
            [buddy.core.hash]
            [bouncer.core :as b]
            [bouncer.validators :as v :refer [defvalidator]]
            [liberator.core :as liberator]
            [liberator.representation :refer [ring-response]]
            [clojure.string :as str]
            [ring.util.response :refer [response content-type header redirect]]
            (job-streamer.control-bus [validation :refer [validate]]
                                      [util :refer [parse-body]])
            (job-streamer.control-bus.component [datomic :as d]
                                                [token :as token])))

(defn- auth-by-password [datomic user-id password app-name]
  (when (and (not-empty user-id) (not-empty password))
    (when-let [user (d/query datomic
                             '{:find [(pull ?s [:*]) .]
                               :in [$ ?uname ?passwd ?app-name]
                               :where [[?s :user/id ?uname]
                                       [?s :user/salt ?salt]
                                       [(concat ?salt ?passwd) ?passwd-seq]
                                       [(into-array Byte/TYPE ?passwd-seq) ?passwd-bytes]
                                       [(buddy.core.hash/sha256 ?passwd-bytes) ?hash]
                                       [(buddy.core.codecs/bytes->hex ?hash) ?hash-hex]
                                       [?s :user/password ?hash-hex]
                                       [?member :member/user ?s]
                                       [?app :application/members ?member]
                                       [?app :application/name ?app-name]]}
                             user-id password app-name)]
      (let [permissions (->> (d/query datomic
                                      '{:find [?permission]
                                        :in [$ ?app-name ?user-id]
                                        :where [[?member :member/user ?user]
                                                [?member :member/rolls ?roll]
                                                [?roll :roll/permissions ?permission]
                                                [?user :user/id ?user-id]
                                                [?app :application/members ?member]
                                                [?app :application/name ?app-name]]}
                                      app-name user-id)
                             (apply concat)
                             set)]
        (assoc user :permissions permissions)))))

(defvalidator unique-name-validator
  {:default-message-format "%s is used by someone."}
  [id datomic]
  (nil? (d/query datomic
                 '{:find [?u .]
                   :in [$ ?id]
                   :where [[?u :user/id ?id]]}
                 id)))

(defvalidator exist-roll-validator
  {:default-message-format "%s is used by someone."}
  [roll-name datomic]
  (d/query datomic
           '[:find ?e .
             :in $ ?n
             :where [?e :roll/name ?n]]
           roll-name))

(defn- signup [datomic user roll-name]
  (when-let [user (let [salt (nonce/random-nonce 16)
                        password (some-> (not-empty (:user/password user))
                                         (.getBytes)
                                         (#(into-array Byte/TYPE (concat salt %)))
                                         buddy.core.hash/sha256
                                         buddy.core.codecs/bytes->hex)]
                    (if-not (or password (:user/token user))
                      (throw (Exception.))
                      (merge user
                             {:db/id #db/id[db.part/user -1]}
                             (when password
                               {:user/password password
                                :user/salt salt})
                             (when-let [token (:user/token user)]
                               {:user/token token}))))]
    (let [roll-id (d/query datomic
                           '[:find ?e .
                             :in $ ?n
                             :where [?e :roll/name ?n]]
                           roll-name)
          member-id (d/tempid :db.part/user)
          app (d/query datomic
                       '[:find (pull ?e [*]) .
                         :in $ ?n
                         :where [?e :application/name ?n]]
                       "default")]
      (let [result (d/transact datomic [user
                                        {:db/id member-id
                                         :member/user (select-keys user [:db/id])
                                         :member/rolls [roll-id]}
                                        (update-in (select-keys app [:db/id :application/members]) [:application/members] #(conj % member-id))])]
        (log/infof "Signup %s as %s succeeded." (:user/id user) roll-name)
        result))))

(defn auth-resource
  [{:keys [datomic token] :as component}]
  (liberator/resource
    :available-media-types ["application/edn" "application/json"]
    :allowed-methods [:post :delete]
    :malformed? (fn [ctx]
                  (validate (parse-body ctx)
                            :user/id [v/required [v/matches #"^[\w\-]+$"]]
                            :user/password [v/required [v/matches #"^[\w\-]+$"]]
                            :user/app-name [v/required [v/matches #"^[\w\-]+$"]]))
    :handle-created (fn [{{:keys [user/id user/password user/app-name]} :edn}]
                      (log/infof "Login attempt with parameters : %s." (pr-str {:username id :password "********" :appname app-name}))
                      (if-let [user (auth-by-password datomic id password app-name)]
                        (let [access-token (token/new-token token user)
                              _ (log/infof "Login attempt succeeded with access token : %s." access-token)]
                          (ring-response {:session {:identity (select-keys user [:user/id :permissions])}
                                          :body (pr-str {:token (str access-token)})}))
                        (do (log/info "Login attempt failed because of authentification failure.")
                          (ring-response {:body (pr-str {:messages ["Autification failure."]})}))))
    :handle-no-content (fn [_] (ring-response {:session {}}))))

(defn list-resource
  [{:keys [datomic] :as component}]
  (liberator/resource
    :available-media-types ["application/edn" "application/json"]
    :allowed-methods [:get]
    :handle-ok (fn [_]
                 (d/query datomic
                          '[:find [(pull ?e [:user/id]) ...]
                            :where [?e :user/id]]))))

(defn entry-resource
  [{:keys [datomic] :as component} user-id]
  (liberator/resource
    :available-media-types ["application/edn" "application/json"]
    :allowed-methods [:post :delete]
    :malformed? (fn [ctx]
                  (validate (parse-body ctx)
                            :user/password [[v/required :pre (comp nil? :user/token)]
                                            [v/min-count 8 :message "Password must be at least 8 characters long." :pre (comp nil? :user/token)]]
                            :user/token    [[v/required :pre (comp nil? :user/password)]
                                            [v/matches #"[0-9a-z]{16}" :pre (comp nil? :user/password)]]
                            :user/id       [[v/required]
                                            [v/min-count 3 :message "Username must be at least 3 characters long."]
                                            [v/max-count 20 :message "Username is too long."]
                                            [unique-name-validator datomic]]
                            :roll          [[v/required]
                                            [v/matches #"^[\w\-]+$"]
                                            [exist-roll-validator datomic]]))
    :post! (fn [{user :edn}]
             (let [roll-name (:roll user)
                   user (select-keys user [:user/id :user/password])]
               (signup datomic user roll-name)))
    :delete! (fn [_]
               (d/transact datomic
                           [[:db.fn/retractEntity [:user/id user-id]]]))))

(defrecord Auth [datomic]
  component/Lifecycle

  (start [component]

         ;; Create an initil user and rolls.
         (->> [{:db/id (d/tempid :db.part/user)
                :roll/name "admin"
                :roll/permissions [:permission/read-job
                                   :permission/create-job
                                   :permission/update-job
                                   :permission/delete-job
                                   :permission/execute-job]}
               {:db/id (d/tempid :db.part/user)
                :roll/name "operator"
                :roll/permissions [:permission/read-job
                                   :permission/execute-job]}
               {:db/id (d/tempid :db.part/user)
                :roll/name "watcher"
                :roll/permissions [:permission/read-job]}]
              (filter #(nil? (d/query datomic
                                      '[:find ?e .
                                        :in $ ?n
                                        :where [?e :roll/name ?n]]
                                      (:roll/name %))))
              (d/transact datomic))
         (when-not (d/query datomic
                            '[:find ?e .
                              :in $ ?n
                              :where [?e :user/id ?n]]
                            "admin")
           (signup datomic {:user/id "admin" :user/password "password123"} "admin"))

         component)

  (stop [component]
        component))

(defn auth-component [options]
  (map->Auth options))
