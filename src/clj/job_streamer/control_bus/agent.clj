(ns job-streamer.control-bus.agent
  (:use [org.httpkit.server :only [send!]]
        [liberator.core :only [defresource]])
  (:require [clojure.edn :as edn]
            [org.httpkit.client :as http]
            [clojure.tools.logging :as log]
            [job-streamer.control-bus.model :as model]))

(defonce agents (atom #{}))

(defresource agents-resource
  :available-media-types ["application/edn"]
  :allowed-methods [:get :post]
  :handle-ok (fn [ctx]
               (vec (map #(dissoc % :channel) @agents))))

(defn ready [ch data]
  (log/info "ready" ch data)
  (model/transact [{:db/id #db/id[db.part/user]
                    :agent/instance-id (:instance-id data)
                    :agent/name (:name data)}])
  (swap! agents conj (merge data {:channel ch})))

(defn bye [ch]
  (swap! agents #(remove (fn [agt] (= (:channel agt) ch)) %)))

(defn execute-job [agt execution-request & {:keys [on-error on-success]}]
  (log/info (pr-str execution-request))
  (http/post (str "http://" (:host agt) ":" (:port agt) "/jobs")
             {:body (pr-str execution-request)
              :headers {"Content-Type" "application/edn"}}
             (fn [{:keys [status headers body error]}]
               (cond (and error on-error) (on-error error)
                     on-success (on-success (edn/read-string body))))))

(defn update-execution [agt execution-id & {:keys [on-error on-success]}]
  (http/get (str "http://" (:host agt) ":" (:port agt) "/job-execution/" execution-id)
            {:as :text
             :headers {"Content-Type" "application/edn"}}
            (fn [{:keys [status headers body error]}]
              (cond (and error on-error) (on-error error)
                    on-success (on-success (edn/read-string body))))))

(defn update-step-execution [agt execution-id step-execution-id & {:keys [on-error on-success]}]
  (http/get (str "http://" (:host agt) ":" (:port agt)
                 "/job-execution/" execution-id
                 "/step-execution/" step-execution-id)
            {:as :text
             :headers {"Content-Type" "application/edn"}}
            (fn [{:keys [status headers body error]}]
              (cond (and error on-error) (on-error error)
                    on-success (on-success (edn/read-string body))))))

(defn find-agent-by-channel [ch]
  (first (filter #(= (:channel %) ch) @agents)))

(defn find-agent []
  ;; TODO
  (first @agents))
