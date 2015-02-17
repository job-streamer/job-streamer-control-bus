(ns job-streamer.control-bus.api
  "Define resources for web api."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [liberator.core :refer [defresource]]
            [datomic.api :as d]
            (job-streamer.control-bus (model :as model)
                                      (job :as job)
                                      (broadcast :as broadcast)
                                      (server :as server)
                                      (scheduler :as scheduler)
                                      (dispatcher :as dispatcher)))
  (:use [clojure.core.async :only [chan put! <! go-loop timeout]]
        [liberator.representation :only [ring-response]]
        [job-streamer.control-bus.agent :only [find-agent execute-job] :as ag]
        [ring.util.response :only [header]]))

(defn- body-as-string [ctx]
  (if-let [body (get-in ctx [:request :body])]
    (condp instance? body
      java.lang.String body
      (slurp (io/reader body)))))

(defn- parse-edn [context key]
  (when (#{:put :post} (get-in context [:request :request-method]))
    (try
      (if-let [body (body-as-string context)]
        (let [data (edn/read-string body)]
          [false {key data}])
        false)
      (catch Exception e
        (log/error "fail to parse edn." e)
        (:message (format "IOException: %s" (.getMessage e)))))))

(defn find-latest-execution
  "Find latest from given executions."
  [executions]
  (when executions
    (->> executions
         (sort #(compare (:job-execution/start-time %2)
                         (:job-execution/start-time %1)))
         first)))

(defn find-next-execution
  "Find next execution from a scheduler.
   If a job isn't scheduled, it returns nil."
  [job]
  (when (:job/schedule job)
    (let [next-start (first (scheduler/fire-times (:db/id job)))]
      {:job-execution/start-time next-start})))

(defresource jobs-resource
  :available-media-types ["application/edn"]
  :allowed-methods [:get :post]
  :malformed? #(parse-edn % :job)
  :post! (fn [{job :job}]
           (model/transact (job/edn->datoms job nil))
           "OK")
  :handle-ok (fn [{{{query "q"} :query-params} :request}]
               (let []
                 (vec (map (fn [{job-id :job/id
                                 executions :job/executions
                                 schedule :job/schedule :as job}]
                             {:job/id job-id
                              :job/executions executions
                              :job/latest-execution (find-latest-execution executions)
                              :job/next-execution   (find-next-execution job)})
                           (job/find-all query))))))

(defresource job-resource [job-id]
  :available-media-types ["application/edn"]
  :allowed-methods [:get :put :delete]
  :malformed? #(parse-edn % ::data)
  :exists? (when-let [job (job/find-by-id job-id)]
             {:job job})
  :put! (fn [{job ::data job-id :job}]
          (model/transact (job/edn->datoms job job-id)))
  :delete! (fn [{job-id :job}]
             (model/transact [[:db.fn/retractEntity job-id]]))
  :handle-ok (fn [ctx]
               (let [job (model/pull '[:*
                                       {:job/executions
                                        [:job-execution/start-time
                                         :job-execution/end-time
                                         {:job-execution/batch-status [:db/ident]}
                                         {:job-execution/agent [:agent/name]}]}
                                       {:job/schedule [:schedule/cron-notation]}]
                              (:job ctx))
                     total (count (:job/executions job))
                     success (->> (:job/executions job)
                                  (filter #(= (get-in % [:job-execution/batch-status :db/ident]) 
                                              :batch-status/completed))
                                  count)
                     failure (->> (:job/executions job)
                                  (filter #(= (get-in % [:job-execution/batch-status :db/ident])
                                              :batch-status/failed))
                                  count)
                     average (if (= success 0) 0
                                 (/ (->> (:job/executions job)
                                             (filter #(= (get-in % [:job-execution/batch-status :db/ident])
                                                         :batch-status/completed))
                                             (map #(- (.getTime (:job-execution/end-time %))
                                                      (.getTime (:job-execution/start-time %))))
                                             (reduce +))
                                   success))]
                 (-> job
                     (assoc :job/stats {:total total :success success :failure failure :average average}
                            :job/latest-execution (find-latest-execution (:job/executions job)) 
                            :job/next-execution   (find-next-execution job))
                     (dissoc :job/executions)))))

(defresource executions-resource [job-id]
  :available-media-types ["application/edn"]
  :allowed-methods [:get :post]
  :malformed? #(parse-edn % ::data)
  :exists? (when-let [job (job/find-by-id job-id)]
             {:job job})
  :if-match-star-exists-for-missing? (fn [{job :job}]
                                       (nil? job))
  :post! (fn [ctx]
           (when-let [job (job/find-by-id job-id)]
             (model/transact [{:db/id #db/id[db.part/user -1]
                               :job-execution/batch-status :batch-status/registered
                               :job-execution/job-parameters (pr-str (or (::data ctx) {}))}
                              [:db/add job :job/executions #db/id[db.part/user -1]]])))
  :handle-ok (fn [ctx]
               (job/find-executions job-id)))

(defresource execution-resource [job-id id]
  :available-media-types ["application/edn"]
  :allowed-methods [:get]
  :handle-ok (fn [ctx]
               (job/find-execution job-id id)))

(defresource schedule-resource [job-id]
  :available-media-types ["application/edn"]
  :allowed-methods [:post :delete]
  :malformed? #(parse-edn % ::data)
  :exists? (fn [ctx]
             (:job/schedule (model/pull '[:job/schedule] job-id)))
  :post! (fn [ctx]
           (scheduler/schedule job-id (get-in ctx [::data :schedule/cron-notation])))
  :delete! (fn [ctx]
             (scheduler/unschedule job-id))
  :handle-ok (fn [ctx])
  :handle-exception (fn [{ex :exception}]
                      (ring-response {:status 500
                                      :body (pr-str {:message (.getMessage ex)})})))

(defresource application-resource [app-id]
  :available-media-types ["application/edn"]
  :allowed-methods [:post]
  :malformed? #(parse-edn % ::data)
  :post! (fn [ctx]
           )
  :handle-ok (fn [ctx]))
