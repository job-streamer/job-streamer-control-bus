(ns job-streamer.control-bus.component.dispatcher
  (:require [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [clojure.core.async :refer [chan put! <! go-loop timeout close!]]
            (job-streamer.control-bus.component [agents :as ag]
                                                [apps :as apps]
                                                [datomic :as d]
                                                [jobs :as jobs]))
  (:import [net.unit8.job_streamer.control_bus.bpmn BpmnParser]
           [org.jsoup Jsoup]
           [org.jsoup.nodes Element Node]
           [org.jsoup.parser Tag Parser]))

(defn- restart [{:keys [agents datomic jobs]} execution class-loader-id]
  (log/info "restart:" execution)
  (ag/restart-execution
     agents execution class-loader-id
     :on-success (fn [resp]
                   (d/transact datomic
                               [{:db/id (:db/id execution)
                                 :job-execution/execution-id (:execution-id resp)
                                 :job-execution/batch-status (:batch-status resp)
                                 :job-execution/start-time   (:start-time   resp)}])
                   (ag/update-execution-by-id
                    agents
                    (:db/id execution)
                    :on-success (fn [new-exec]
                                  (jobs/save-execution jobs (:db/id execution) new-exec))))))

(defn- dispatch [{:keys [dispatcher-ch datomic]} agt execution-request]
  (ag/execute-job
   agt execution-request

   :on-error
   (fn [status e]
     (log/error "failure submit job [" (get-in execution-request [:job :job/name])
                "] at host [" (:host agt) "]" e)
     (if (not= status 503)
       (d/transact datomic
                   [{:db/id (:request-id execution-request)
                     :job-execution/agent [:agent/instance-id (:agent/instance-id agt)]
                     :job-execution/batch-status :batch-status/abandoned}])
       (put! dispatcher-ch execution-request)))

   :on-success
   (fn [{:keys [execution-id batch-status start-time] :as res}]
     (if execution-id
       (d/transact datomic
                   [(merge {:db/id (:request-id execution-request)
                            :job-execution/execution-id execution-id
                            :job-execution/agent [:agent/instance-id (:agent/instance-id agt)]
                            :job-execution/batch-status batch-status}
                           (when start-time
                             {:job-execution/start-time start-time}))])
       (d/transact datomic
                   [{:db/id (:request-id execution-request)
                     :job-execution/agent [:agent/instance-id (:agent/instance-id agt)]
                     :job-execution/batch-status :batch-status/abandoned}])))))

(defn submit [{:keys [dispatcher-ch datomic]} execution-request]
  (try
    (put! dispatcher-ch execution-request)
    (d/transact datomic
                [{:db/id (:request-id execution-request)
                  :job-execution/batch-status :batch-status/queued}])
    (catch Exception ex
      (log/error "dispatch failure" ex))))

(defn convert-to-test-job [jobxml-str]
 (let [jobxml (Jsoup/parse  jobxml-str "" (Parser/xmlParser))
       steps (.select jobxml "step")
       batchlets (.select jobxml "step > batchlet")
       chunks (.select jobxml "step > chunk")]
     (.remove batchlets)
     (.remove chunks)
   (doall
     (map #(.appendChild % (some-> (Tag/valueOf "batchlet") (Element. "") (.attr "ref" "org.jobstreamer.batch.TestBatchlet"))) steps))
   (.toString jobxml)))

(defn make-job [job-bpmn-xml test?]
  (let [job (str "<?xml version=\"1.0\" encoding=\"UTF-8\"?> " \newline (some-> (new BpmnParser) (.parse job-bpmn-xml) .toString))]
    (if test?
      (convert-to-test-job job)
      job)))

(defn submitter [{:keys [jobs apps datomic submitter-ch] :as dispatcher}]
  (go-loop []
    (when-let [_ (<! submitter-ch)]
      (let [undispatched (jobs/find-undispatched jobs)]
        (doseq [[execution-request job-bpmn-xml parameter test?] undispatched]
          (submit dispatcher
                  {:request-id execution-request
                   :class-loader-id (:application/class-loader-id
                                     (apps/find-by-name apps "default"))
                   :job (make-job job-bpmn-xml test?)
                   :restart? (= (some-> (d/pull datomic
                                                '[:job-execution/batch-status]
                                                execution-request)
                                        :job-execution/batch-status
                                        :db/ident)
                                :batch-status/undispatched)
                   :test? test?
                   :parameters parameter}))
        (<! (timeout 2000))
        (put! submitter-ch :continue)
        (recur)))))

(defrecord Dispatcher [datomic agents]
  component/Lifecycle
  (start [component]
    (let [component (assoc component
                           :submitter-ch  (chan)
                           :dispatcher-ch (chan))
          main-loop (go-loop []
                      (when-let [execution-request (<! (:dispatcher-ch component))]
                        (log/info "Dispatch request for " execution-request)
                        (if (:restart? execution-request)
                          (restart component
                                   (d/pull datomic
                                           '[:*
                                             {:job-execution/agent [:*]}]
                                           (:request-id execution-request))
                                   (:class-loader-id execution-request))
                          (loop [agt (ag/find-agent agents), log-interval 0]
                            (if agt
                              (dispatch component
                                        agt execution-request)
                              (do
                                (if (= (mod log-interval 10) 0)
                                  (log/info "No available agents for " execution-request))
                                (<! (timeout 3000))
                                (put! (:dispatcher-ch component) execution-request)
                                ;(recur (ag/find-agent agents) (inc log-interval))
                                ))))
                        (recur)))
          submit-loop (submitter component)]
      (put! (:submitter-ch component) :start)
      (assoc component
             :main-loop main-loop
             :submit-loop submit-loop)))

  (stop [component]
    (when-let [dispatcher-ch (:dispatcher-ch component)]
      (close! dispatcher-ch))
    (when-let [submitter-ch (:submitter-ch component)]
      (close! submitter-ch))
    (when-let [main-loop (:main-loop component)]
      (close! main-loop))
    (when-let [submit-loop (:submit-loop component)]
      (close! submit-loop))
    (dissoc component :dispatch-ch :main-loop :submit-loop)))

(defn dispatcher-component [options]
  (map->Dispatcher options))
