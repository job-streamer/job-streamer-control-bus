(ns job-streamer.control-bus.component.auth
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [buddy.core.nonce :as nonce]
            [buddy.core.hash]
            [bouncer.core :as b]
            [bouncer.validators :as v :refer [defvalidator]]
            [clojure.string :as str]
            [ring.util.response :refer [response content-type header redirect]]
            (job-streamer.control-bus.component [datomic :as d]
                                                [token :as token])))

(defn auth-by-password [datomic user-id password]
  (when (and (not-empty user-id) (not-empty password))
    (d/query datomic
             '{:find [(pull ?s [:*]) .]
               :in [$ ?uname ?passwd]
               :where [[?s :user/id ?uname]
                       [?s :user/salt ?salt]
                       [(concat ?salt ?passwd) ?passwd-seq]
                       [(into-array Byte/TYPE ?passwd-seq) ?passwd-bytes]
                       [(buddy.core.hash/sha256 ?passwd-bytes) ?hash]
                       [(buddy.core.codecs/bytes->hex ?hash) ?hash-hex]
                       [?s :user/password ?hash-hex]]}
             user-id password)))

(defvalidator unique-name-validator
  {:default-message-format "%s is used by someone."}
  [id datomic]
  (nil? (d/query datomic
                 '{:find [?u .]
                   :in [$ ?id]
                   :where [[?u :user/id ?id]]}
                 id)))

(defn validate-user [datomic user]
  (b/validate user
              :user/password [[v/required :pre (comp nil? :user/token)]
                              [v/min-count 8 :message "Password must be at least 8 characters long." :pre (comp nil? :user/token)]]
              :user/token    [[v/required :pre (comp nil? :user/password)]
                              [v/matches #"[0-9a-z]{16}" :pre (comp nil? :user/password)]]
              :user/id       [[v/required]
                              [v/min-count 3 :message "Username must be at least 3 characters long."]
                              [v/max-count 20 :message "Username is too long."]
                              [unique-name-validator datomic]]))

(defn signup [datomic user]
  (let [[result map] (validate-user datomic user)]
    (if-let [error-map (:bouncer.core/errors map)]
      (println error-map) ;; (signup-view {:error-map error-map :params user})
      (let [salt (nonce/random-nonce 16)
            password (some-> (not-empty (:user/password user))
                             (.getBytes)
                             (#(into-array Byte/TYPE (concat salt %)))
                             buddy.core.hash/sha256
                             buddy.core.codecs/bytes->hex)]
        (if-not (or password (:user/token user))
          (throw (Exception.)))
        (d/transact datomic
                    [(merge user
                            {:db/id #db/id[db.part/user -1]}
                            (when password
                              {:user/password password
                               :user/salt salt})
                            (when-let [token (:user/token user)]
                              {:user/token token}))])))))

(defn login [{:keys [datomic token]} {{:keys [username password next back]} :params}]
  (if-let [user (auth-by-password datomic username password)]
    (let [access-token (token/new-token token user)]
      (-> (redirect next)
          (assoc-in [:session :identity] (select-keys user [:id]))))
    (redirect (str back "?error=true"))))

(defrecord Auth [datomic]
  component/Lifecycle

  (start [component]
         ;; create the initil user.
         (if-let [user-id (d/query datomic
                                   '[:find ?e .
                                     :in $ ?n
                                     :where [?e :user/id ?n]]
                                   "admin")]
           nil
           (signup datomic {:user/id "admin" :user/password "password123"}))
         component)

  (stop [component]
        component))

(defn auth-component [options]
  (map->Auth options))
