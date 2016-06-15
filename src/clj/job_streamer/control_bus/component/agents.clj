(ns job-streamer.control-bus.component.agents
  (:require [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [chan put! <! close! go-loop timeout]]
            [com.stuartsierra.component :as component]
            [org.httpkit.client :as http]
            [liberator.core :as liberator]
            [liberator.representation :refer [ring-response]]
            (job-streamer.control-bus [model :as model]
                                      [rrd :as rrd]
                                      [apps :as apps])
            (job-streamer.control-bus.component [datomic :as d]))
  (:import [java.util UUID]
           [java.io ByteArrayInputStream]))

(defn list-resource [{:keys [agents]}]
  (liberator/resource
   :available-media-types ["application/edn"]
   :allowed-methods [:get]
   :handle-ok (fn [ctx]
                (->> (vals @agents)
                    (map #(dissoc % :agent/channel))
                    vec))))

(defn monitor-resource [{:keys [agents]} instance-id type cycle]
  (liberator/resource
   :available-media-types ["image/png"]
   :allowed-methods [:get]
   :handle-ok (fn [ctx]
                (when-let [agt (get @agents (UUID/fromString instance-id))]
                  (ring-response
                   {:status 200
                    :headers {"Content-Type" "image/png"}
                    :body (-> (rrd/render-graph agt type)
                              (ByteArrayInputStream.))})))))

(defn entry-resource [{:keys [agents datomic]} instance-id & [cmd]]
  (liberator/resource
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
                      (assoc
                       :agent/executions
                       (->> (d/query datomic
                                     '{:find [(pull ?execution
                                                    [:job-execution/create-time
                                                     :job-execution/start-time
                                                     :job-execution/end-time
                                                     {:job-execution/batch-status [:db/ident]}])
                                              (pull ?job [:job/name])]
                                       :in [$ ?instance-id]
                                       :where [[?job :job/executions ?execution]
                                               [?execution :job-execution/agent ?agt]
                                               [?agt :agent/instance-id ?instance-id]]}
                                     (UUID/fromString instance-id))
                            (map #(apply merge %))
                            (sort-by :job-execution/create-time #(compare %2 %1))
                            (take 20)
                            vec))
                      (dissoc :agent/channel))))))

(defn ready [{:keys [datomic agents]} ch data]
  (log/info "ready" ch data)
  (when-not (d/query datomic
                     '[:find ?e .
                       :in $ ?instance-id
                       :where [?e :agent/instance-id ?instance-id]]
                     (:agent/instance-id data))
    (d/transact datomic
                [{:db/id #db/id[db.part/user]
                  :agent/instance-id (:agent/instance-id data)
                  :agent/name (:agent/name data)}]))
  (swap! agents assoc
         (:agent/instance-id data)
         (merge data {:agent/channel ch
                      :agent/status :ready})))

(defn bye [{:keys [agents]} ch]
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

(defn stop-execution [{:keys [agents]} execution & {:keys [on-error on-success]}]
  (let [instance-id (get-in execution [:job-execution/agent :agent/instance-id])]
    (when-let [agt (get @agents instance-id)]
      (http/put (str "http://" (:agent/host agt) ":" (:agent/port agt)
                      "/job-execution/" (:job-execution/execution-id execution) "/stop")
                 {:as :text
                  :body (pr-str {})
                  :headers {"Content-Type" "application/edn"}}
                 (fn [{:keys [status headers body error]}]
                   (cond (or (>= status 400) error) (when on-error (on-error error))
                         on-success (on-success (edn/read-string body))))))))

(defn abandon-execution [{:keys [datomic agents]} execution & {:keys [on-error on-success]}]
  (let [instance-id (get-in execution [:job-execution/agent :agent/instance-id])]
    (if-let [agt (get @agents instance-id)]
      (http/put (str "http://" (:agent/host agt) ":" (:agent/port agt)
                      "/job-execution/" (:job-execution/execution-id execution) "/abandon")
                 {:as :text
                  :body (pr-str {})
                  :headers {"Content-Type" "application/edn"}}
                 (fn [{:keys [status headers body error]}]
                   (cond (or (>= status 400) error) (when on-error (on-error error))
                         on-success (on-success (edn/read-string body)))))
      (d/transact datomic
                  [{:db/id (:db/id execution)
                    :job-execution/batch-status
                    :batch-status/abandoned}]))))

(defn restart-execution [{:keys [agents]} execution & {:keys [on-error on-success]}]
  (let [instance-id (get-in execution [:job-execution/agent :agent/instance-id])]
    (when-let [agt (get @agents instance-id)]
      (http/put (str "http://" (:agent/host agt) ":" (:agent/port agt)
                      "/job-execution/" (:job-execution/execution-id execution) "/restart")
                 {:as :text
                  :body (pr-str {:parameters {}
                                 :class-loader-id (:application/class-loader-id (apps/find-by-name "default"))})
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

(defn update-execution-by-id [{:keys [agents datomic]} id & {:keys [on-error on-success]}]
  (let [job-execution (d/pull datomic
                              '[:job-execution/execution-id
                                {:job-execution/agent [:agent/instance-id]}]
                              id)
        instance-id (get-in job-execution [:job-execution/agent :agent/instance-id])]
    (when-let [agt (get @agents instance-id)]
      (update-execution agt
                        (:job-execution/execution-id job-execution)
                        :on-error on-error :on-success on-success))))

(defn update-step-execution [agt execution-id step-execution-id & {:keys [on-error on-success]}]
  (http/get (str "http://" (:agent/host agt) ":" (:agent/port agt)
                 "/job-execution/" execution-id
                 "/step-execution/" step-execution-id)
            {:as :text
             :headers {"Content-Type" "application/edn"}}
            (fn [{:keys [status headers body error]}]
              (cond (or (>= status 400) error) (when on-error (on-error error))
                    on-success (on-success (edn/read-string body))))))

(defn available-agents [{:keys [agents]}]
  (vals @agents))

(defn find-agent-by-channel [{:keys [agents]} ch]
  (first (filter #(= (:agent/channel %) ch) (vals @agents))))

(defn find-agent [component]
  (->> (vals @(:agents component))
       (filter #(= (:agent/status %) :ready))
       (sort #(or (< (get-in %1 [:agent/jobs :running]) (get-in %2 [:agent/jobs :running]))
                  (and (= (get-in %1 [:agent/jobs :running]) (get-in %2 [:agent/jobs :running]))
                       (< (get-in %1 [:agent/stats :cpu :system :load-average])
                          (get-in %2 [:agent/stats :cpu :system :load-average])))))
       first))

(defrecord Agents []
  component/Lifecycle

  (start [component]
    (let [agents (atom {})
          main-loop (go-loop []
                      (doseq [agt (vals @agents)]
                        (http/get (str "http://" (:agent/host agt)
                                       ":" (:agent/port agt)
                                       "/spec")
                                  {:as :text
                                   :headers {"Content-Type" "application/edn"}}
                                  (fn [{:keys [status headers body error]}]
                                    (when-not error
                                      (let [spec (edn/read-string body)]
                                        (swap! agents assoc (:agent/instance-id agt) (merge agt spec))
                                        (rrd/update agt))))))
                      (<! (timeout 60000))
                      (recur))]
      (assoc component :agents agents)))

  (stop [component]
    (if-let [main-loop (:main-loop component)]
      (close! main-loop))
    (dissoc component :main-loop :agents)))

(defn agents-component [options]
  (map->Agents options))
