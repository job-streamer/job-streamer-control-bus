(ns job-streamer.control-bus.agent
  (:use [liberator.core :only [defresource]]
        [clojure.core.async :only [chan put! <! go-loop timeout]])
  (:require [clojure.edn :as edn]
            [org.httpkit.client :as http]
            [clojure.tools.logging :as log]
            [job-streamer.control-bus.model :as model]))

(defonce agents (ref #{}))

(defresource agents-resource
  :available-media-types ["application/edn"]
  :allowed-methods [:get :post]
  :handle-ok (fn [ctx]
               (vec (map #(dissoc % :channel) @agents))))

(defn ready [ch data]
  (log/info "ready" ch data)
  (when-not (model/query '[:find ?e .
                       :in $ ?instance-id
                       :where [?e :agent/instance-id ?instance-id]]
                     (:instance-id data))
    (model/transact [{:db/id #db/id[db.part/user]
                    :agent/instance-id (:instance-id data)
                    :agent/name (:name data)}]))
  (dosync
   (alter agents conj (merge data {:channel ch}))))

(defn bye [ch]
  (dosync
   (alter agents #(remove (fn [agt] (= (:channel agt) ch)) %))))

(defn execute-job
  "Send a request for job execution to an agent."
  [agt execution-request & {:keys [on-error on-success]}]
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
              (cond (and error on-error)
                    (on-error error)
                    on-success (on-success (edn/read-string body))))))

(defn available-agents []
  @agents)

(defn find-agent-by-channel [ch]
  (first (filter #(= (:channel %) ch) @agents)))

(defn find-agent []
  (->> @agents
       (sort #(or (< (get-in %1 [:jobs :running]) (get-in %2 [:jobs :running]))
                  (and (= (get-in %1 [:jobs :running]) (get-in %2 [:jobs :running]))
                       (< (get-in %1 [:cpu :system :load-average]) (get-in %2 [:cpu :system :load-average])))))
       first))

(defn start-monitor []
  (go-loop []
    (doseq [agt @agents]
      (http/get (str "http://" (:host agt) ":" (:port agt) "/spec")
                {:as :text
                 :headers {"Content-Type" "application/edn"}}
                (fn [{:keys [status headers body error]}]
                  (when-not error
                    (let [spec (edn/read-string body)]
                      (dosync
                       (log/info "AgentMonitor" @agents)
                       (alter agents disj agt)
                       (alter agents conj (merge agt spec))))))))
    (<! (timeout 60000))
    (recur)))
