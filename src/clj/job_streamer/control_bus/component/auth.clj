(ns job-streamer.control-bus.component.auth
  (:require [clojure.tools.logging :as log]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clojure.edn :as edn]
            [com.stuartsierra.component :as component]
            [bouncer.core :as b]
            [bouncer.validators :as v]
            [liberator.core :as liberator]
            [clojure.string :as str]
            [clj-time.format :as f]
            [liberator.representation :refer [ring-response]]
            [ring.util.response :refer [response content-type header]]
            (job-streamer.control-bus [notification :as notification]
                                      [validation :refer [validate]]
                                      [util :refer [parse-body edn->datoms to-int]])
            (job-streamer.control-bus.component [datomic :as d]
                                                [token :as token]))
  (:import [java.util Date]))

(defn- auth-by-password [datomic username password])

(defn auth-resource [{:keys [datomic token]}]
  (liberator/resource
    :available-media-types ["application/edn" "application/json"]
    :allowed-methods [:post]
    :post! (fn [ctx]
             (println "!!!" ctx)
             (let [user {:name "admin" :password "admin"}
                   access-token (token/new-token token user)]
               {::post-response (merge user
                                       {:token-type "bearer"
                                        :access-token access-token})}))
    :handle-created (fn [ctx]
                      (::post-response ctx))))

(defrecord Auth []
  component/Lifecycle

  (start [component]
         component)

  (stop [component]
        component))

(defn auth-component [options]
  (map->Auth options))
