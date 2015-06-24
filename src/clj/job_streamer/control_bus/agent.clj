(ns job-streamer.control-bus.agent
  (:use [liberator.core :only [defresource]]
        [clojure.core.async :only [chan put! <! go-loop timeout]]
        [liberator.representation :only [ring-response]])
  (:require [clojure.edn :as edn]
            [org.httpkit.client :as http]
            [clojure.tools.logging :as log]
            (job-streamer.control-bus [model :as model]
                                      [rrd :as rrd]))
  (:import [java.util UUID]
           [java.io ByteArrayInputStream]))

(defonce agents (atom {}))

(defresource agents-resource
  :available-media-types ["application/edn"]
  :allowed-methods [:get]
  :handle-ok (fn [ctx]
               (->> (vals @agents)
                    (map #(dissoc % :agent/channel))
                    vec)))

(defresource agent-monitor-resource [instance-id type cycle]
  :available-media-types ["image/png"]
  :allowed-methods [:get]
  :handle-ok (fn [ctx]
               (when-let [agt (get @agents (UUID/fromString instance-id))]
                 (ring-response
                  {:status 200
                   :headers {"Content-Type" "image/png"}
                   :body (-> (rrd/render-graph agt type)
                             (ByteArrayInputStream.))}))))

(defresource agent-resource [instance-id & [cmd]]
  :available-media-types ["application/edn"]
  :allowed-methods [:get :put]
  :put! (fn [ctx]
          (when-let [uuid (UUID/fromString instance-id)]
            (case cmd
            :block (swap! agents assoc-in [uuid :status] :blocking)
            :unblock (swap! agents assoc-in [uuid :status] :ready))))
  :handle-ok (fn [ctx]
               (when-let [agt (get @agents (UUID/fromString instance-id))]
                 (-> agt
                     (assoc :agent/executions
                            (->> (model/query '{:find [(pull ?execution [:job-execution/create-time
                                                                       :job-execution/start-time
                                                                       :job-execution/end-time
                                                                       {:job-execution/batch-status [:db/ident]}])
                                                     (pull ?job [:job/name])]
                                              :in [$ ?instance-id]
                                              :where [[?job :job/executions ?execution]
                                                      [?execution :job-execution/agent ?agt]
                                                      [?agt :agent/instance-id ?instance-id]]} (UUID/fromString instance-id))
                                 (map #(apply merge %))
                                 vec))
                     (dissoc :agent/channel)))))

(defn ready [ch data]
  (log/info "ready" ch data)
  (when-not (model/query '[:find ?e .
                           :in $ ?instance-id
                           :where [?e :agent/instance-id ?instance-id]]
                         (:agent/instance-id data))
    (model/transact [{:db/id #db/id[db.part/user]
                      :agent/instance-id (:agent/instance-id data)
                      :agent/name (:agent/name data)}]))
  (swap! agents assoc
         (:agent/instance-id data)
         (merge data {:agent/channel ch
                      :agent/status :ready})))

(defn bye [ch]
  (when-let [instance-ids (some->> (vals @agents)
                                   (filter #(= (:agent/channel %) ch))
                                   (map :agent/instance-id))]
    (doseq [instance-id instance-ids]
      (swap! agents dissoc instance-id))))

(defn execute-job
  "Send a request for job execution to an agent."
  [agt execution-request & {:keys [on-error on-success]}]
  (log/info (pr-str execution-request))
  (http/post (str "http://" (:agent/host agt) ":" (:agent/port agt) "/jobs")
             {:body (pr-str execution-request)
              :headers {"Content-Type" "application/edn"}}
             (fn [{:keys [status headers body error]}]
               (cond (or (>= status 400) error)
                     (when on-error (on-error status error)) 
                     on-success (on-success (edn/read-string body))))))

(defn stop-execution [execution-id & {:keys [on-error on-success]}]
  (let [instance-id (some-> (model/pull '[{:job-execution/agent [:agent/instance-id]}] execution-id)
                            (get-in [:job-execution/agent :agent/instance-id]))]
    (when-let [agt (get @agents instance-id)]
      (http/post (str "http://" (:agent/host agt) ":" (:agent/port agt) "/job-execution/" execution-id "/stop")
                 {:as :text
                  :body (pr-str {})
                  :headers {"Content-Type" "application/edn"}}
                 (fn [{:keys [status headers body error]}]
                   (cond (or (>= status 400) error) (when on-error (on-error error))
                         on-success (on-success (edn/read-string body))))))))

(defn abandon-execution [execution-id & {:keys [on-error on-success]}]
  (let [instance-id (some-> (model/pull '[{:job-execution/agent [:agent/instance-id]}] execution-id)
                            (get-in [:job-execution/agent :agent/instance-id]))]
    (when-let [agt (get @agents instance-id)]
      (http/post (str "http://" (:agent/host agt) ":" (:agent/port agt) "/job-execution/" execution-id "/abandon")
                 {:as :text
                  :body (pr-str {})
                  :headers {"Content-Type" "application/edn"}}
                 (fn [{:keys [status headers body error]}]
                   (cond (or (>= status 400) error) (when on-error (on-error error))
                         on-success (on-success (edn/read-string body))))))))

(defn update-execution [agt execution-id & {:keys [on-error on-success]}]
  (log/debug "update-execution" execution-id agt)
  (http/get (str "http://" (:agent/host agt) ":" (:agent/port agt) "/job-execution/" execution-id)
            {:as :text
             :headers {"Content-Type" "application/edn"}}
            (fn [{:keys [status headers body error]}]
              (cond (or (>= status 400) error) (when on-error (on-error error))
                    on-success (on-success (edn/read-string body))))))

(defn update-execution-by-id [id & {:keys [on-error on-success]}]
  (let [job-execution (model/pull '[:job-execution/execution-id {:job-execution/agent [:agent/instance-id]}] id)
        instance-id (get-in [:job-execution/agent :agent/instance-id] job-execution)]
    (when-let [agt (get @agents instance-id)]
      (update-execution agt (:job-execution/execution-id job-execution) :on-error on-error :on-success on-success))))

(defn update-step-execution [agt execution-id step-execution-id & {:keys [on-error on-success]}]
  (http/get (str "http://" (:agent/host agt) ":" (:agent/port agt)
                 "/job-execution/" execution-id
                 "/step-execution/" step-execution-id)
            {:as :text
             :headers {"Content-Type" "application/edn"}}
            (fn [{:keys [status headers body error]}]
              (cond (or (>= status 400) error) (when on-error (on-error error))
                    on-success (on-success (edn/read-string body))))))

(defn available-agents []
  (vals @agents))

(defn find-agent-by-channel [ch]
  (first (filter #(= (:agent/channel %) ch) (vals @agents))))

(defn find-agent []
  (->> (vals @agents)
       (filter #(= (:agent/status %) :ready))
       (sort #(or (< (get-in %1 [:agent/jobs :running]) (get-in %2 [:agent/jobs :running]))
                  (and (= (get-in %1 [:agent/jobs :running]) (get-in %2 [:agent/jobs :running]))
                       (< (get-in %1 [:agent/stats :cpu :system :load-average])
                          (get-in %2 [:agent/stats :cpu :system :load-average])))))
       first))

(defn start-monitor []
  (go-loop []
    (doseq [agt (vals @agents)]
      (http/get (str "http://" (:agent/host agt) ":" (:agent/port agt) "/spec")
                {:as :text
                 :headers {"Content-Type" "application/edn"}}
                (fn [{:keys [status headers body error]}]
                  (when-not error
                    (let [spec (edn/read-string body)]
                      (swap! agents assoc (:agent/instance-id agt) (merge agt spec))
                      (rrd/update agt))))))
    (<! (timeout 60000))
    (recur)))
