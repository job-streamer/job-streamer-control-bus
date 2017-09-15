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
            [clojure.data.json :as json]
            [clojure.data.codec.base64 :as base64]
            [org.httpkit.client :as http]
            [ring.util.response :refer [response content-type header redirect]]
            [ring.util.codec :refer [form-encode]]
            (job-streamer.control-bus [validation :refer [validate]]
                                      [util :refer [parse-body generate-token]])
            (job-streamer.control-bus.component [datomic :as d]
                                                [token :as token])))

(defn- find-permissions [datomic user-id app-name]
  (->> (d/query datomic
                '{:find [?permission]
                  :in [$ ?app-name ?user-id]
                  :where [[?member :member/user ?user]
                          [?member :member/roles ?role]
                          [?role :role/permissions ?permission]
                          [?user :user/id ?user-id]
                          [?app :application/members ?member]
                          [?app :application/name ?app-name]]}
                app-name user-id)
       (apply concat)
       set))

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

      (let [permissions (find-permissions datomic user-id app-name)]
        (assoc user :permissions permissions)))))

(defn- find-user [datomic user-id app-name]
  (when (not-empty user-id)
    (when-let [user (d/query datomic
                             '{:find [(pull ?s [:*]) .]
                               :in [$ ?uname ?app-name]
                               :where [[?s :user/id ?uname]
                                       [?member :member/user ?s]
                                       [?app :application/members ?member]
                                       [?app :application/name ?app-name]]}
                             user-id app-name)]
      (let [permissions (find-permissions datomic user-id app-name)]
        (assoc user :permissions permissions)))))

(defvalidator unique-name-validator
  {:default-message-format "%s is used by someone."}
  [id datomic]
  (nil? (d/query datomic
                 '{:find [?u .]
                   :in [$ ?id]
                   :where [[?u :user/id ?id]]}
                 id)))

(defvalidator exist-role-validator
  {:default-message-format "%s is used by someone."}
  [role-name datomic]
  (d/query datomic
           '[:find ?e .
             :in $ ?n
             :where [?e :role/name ?n]]
           role-name))

(defn signup [datomic user role-name]
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
    (let [role-id (d/query datomic
                           '[:find ?e .
                             :in $ ?n
                             :where [?e :role/name ?n]]
                           role-name)
          member-id (d/tempid :db.part/user)
          app (d/query datomic
                       '[:find (pull ?e [*]) .
                         :in $ ?n
                         :where [?e :application/name ?n]]
                       "default")]
      (let [result (d/transact datomic [user
                                        {:db/id member-id
                                         :member/user (select-keys user [:db/id])
                                         :member/roles [role-id]}
                                        (update-in (select-keys app [:db/id :application/members]) [:application/members] #(conj % member-id))])]
        (log/infof "Signup %s as %s succeeded." (:user/id user) role-name)
        result))))

(defn fetch-access-token [{:keys [oauth-providers control-bus-url]} provider-id state code]
  (when (and (not-empty code) (oauth-providers provider-id))
    (log/info "Fetch access-token with code :" code)
    (let [{:keys [domain token-endpoint client-id client-secret]} (oauth-providers provider-id)
          url (str domain "/" token-endpoint)
          query (-> (merge {:code code
                            :state state
                            :grant_type "authorization_code"
                            :client_id client-id
                            :redirect_uri (str control-bus-url "/oauth/" provider-id "/cb")}
                           (when client-secret {:client_secret client-secret}))
                    form-encode)
          auth-header (->> (str client-id ":" client-secret)
                           .getBytes
                           base64/encode
                           String.
                           (str "Basic "))
          {:keys [body status] :as res} @(http/post (str url "?" query)
                                                     {:body query
                                                      :headers {"Accept" "application/json"
                                                                "Authorization" auth-header
                                                                "Content-Type" "application/x-www-form-urlencoded"}})]
      (when (= 200 status)
        (let [{:keys [access_token]} (json/read-str body :key-fn keyword)]
          (when access_token
            (log/info "access-token :" access_token)
            access_token))))))

(defn auth-resource
  [{:keys [datomic token] :as component}]
  (liberator/resource
    :available-media-types ["application/edn" "application/json"]
    :allowed-methods [:post :delete]
    :malformed? (fn [ctx]
                  (validate (parse-body ctx)
                            :user/id [v/required [v/matches #"^[\w\-]+$"]]
                            :user/password [v/required [v/matches #"^[\w\-]+$"]]))
    :handle-created (fn [{{:keys [user/id user/password]} :edn}]
                      (let [appname "default"]
                        (log/infof "Login attempt with parameters : %s." (pr-str {:username id :password "********" :appname appname}))
                        (if-let [user (auth-by-password datomic id password appname)]
                          (let [access-token (token/new-token token user)
                                _ (log/infof "Login attempt succeeded with access token : %s." access-token)]
                            (ring-response {:session {:identity (select-keys user [:user/id :permissions])}
                                            :body (pr-str {:token (str access-token)})}))
                          (do (log/info "Login attempt failed because of authentication failure.")
                            (ring-response {:status 401 :body (pr-str {:messages ["Authentication failure."]})})))))
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
                            :role          [[v/required]
                                            [v/matches #"^[\w\-]+$"]
                                            [exist-role-validator datomic]]))
    :post! (fn [{user :edn}]
             (let [role-name (:role user)
                   user (select-keys user [:user/id :user/password])]
               (signup datomic user role-name)))
    :delete! (fn [_]
               (d/transact datomic
                           [[:db.fn/retractEntity [:user/id user-id]]]))))

(defn oauth-resource [{:keys [oauth-providers]}]
  (liberator/resource
   :available-media-types ["application/edn" "application/json"]
   :allowed-methods [:get]
   :exists? (fn [_]
              {:providers (->> oauth-providers
                               (map (fn [[id provider]]
                                      [id (select-keys provider [:name :class-name])]))
                               (into {}))})
   :handle-ok (fn [{:keys [providers]}]
                providers)))

(defn redirect-to-auth-provider [{:keys [oauth-providers control-bus-url console-url]} provider-id]
  (fn [request]
    (if-let [{:keys [domain auth-endpoint client-id client-secret scope]} (oauth-providers provider-id)]
      (let [state (generate-token)
            session-with-state (assoc (:session request) :state state)
            url (str domain "/" auth-endpoint)
            query (merge {:client_id client-id
                          :response_type "code"
                          :redirect_uri (str control-bus-url "/oauth/" provider-id "/cb")
                          :state state}
                         (when client-secret {:client_secret client-secret})
                         (when scope {:scope scope}))]
        (log/info "Redirect to auth provider :" provider-id)
        (-> (str url "?" (form-encode query))
            redirect
            (assoc :session session-with-state)))
      (do (log/infof "Redirect attempt to auth provider, %s, failed because of no configuration." provider-id)
          (-> (redirect (str console-url "/login"))
              (assoc :body (pr-str {:messages [(str "No configuration : " provider-id)]})))))))

(defn oauth-callback [{:keys [datomic console-url] :as auth} provider-id]
  (fn [request]
    (let [app-name "default"
          {:keys [state code error]} (:params request)
          session-state (get-in request [:session :state])]
      (if (and (some? code)
               (= state session-state))
        (if-let [token (fetch-access-token auth provider-id state code)]
          (let [identity (-> (find-user datomic "guest" app-name)
                             (select-keys [:user/id :permissions]))]
            (log/info "Login attempt succeeded :" (:user/id identity))
            (-> (redirect console-url)
                (assoc-in [:session :identity] identity)))
          (redirect (str console-url "/login")))
        (redirect (str console-url "/login"))))))

(defrecord Auth [datomic]
  component/Lifecycle

  (start [component]
         component)

  (stop [component]
        component))

(defn auth-component [options]
  (map->Auth options))
