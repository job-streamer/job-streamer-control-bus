(ns job-streamer.control-bus.util
  (:require [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [datomic.api :as d])
  (:import [org.jsoup Jsoup]))

(defn to-int [n default-value]
  (if (nil? n)
    default-value
    (condp = (type n)
      String (Integer/parseInt n)
      Number (int n))))

(defn edn->datoms
  "Convert a format from EDN to datom."
  [job job-id]
  (let [datoms (atom [])
        step-refs (doall
                   (for [step (->> (:job/components job)
                                   (filter #(find % :step/name)))]
                       (let [id (d/tempid :db.part/user)]
                         (swap! datoms conj
                                (merge
                                 {:db/id id
                                  :step/name (:step/name step)
                                  :step/start-limit (get job :step/start-limit 0)
                                  :step/allow-start-if-complete? (get job :step/allow-start-if-complete? false)}
                                 (when-let [batchlet (:step/batchlet step)]
                                   (let [batchlet-id (d/tempid :db.part/user)]
                                     (swap! datoms conj {:db/id batchlet-id :batchlet/ref (:batchlet/ref batchlet)})
                                     {:step/batchlet batchlet-id}))))
                         id)))
        job-id (or job-id (d/tempid :db.part/user)) ]
    (concat [{:db/id job-id
              :job/name (:job/name job)
              :job/restartable? (get job :job/restartable? true) 
              :job/edn-notation (pr-str job)
              :job/steps step-refs
              :job/exclusive? (get job :job/exclusive? false)}]
            (when-let [time-monitor (:job/time-monitor job)]
              [(assoc time-monitor :db/id #db/id[db.part/user -1])
               [:db/add job-id :job/time-monitor #db/id[db.part/user -1]]])
            @datoms)))

(declare xml->components)

(defn xml->batchlet [batchlet]
  (merge {}
         (when-let [ref (not-empty (.attr batchlet "ref"))]
           {:batchlet/ref ref})))

(defn xml->chunk [chunk]
  (merge {}
         (when-let [checkpoint-policy (not-empty (.attr chunk "checkpoint-policy"))]
           {:chunk/checkpoint-policy checkpoint-policy})
         (when-let [item-count (not-empty (.attr chunk "item-count"))]
           {:chunk/item-count item-count})
         (when-let [time-limit (not-empty (.attr chunk "time-limit"))]
           {:chunk/time-limit time-limit})
         (when-let [skip-limit (not-empty (.attr chunk "skip-limit"))]
           {:chunk/skip-limit skip-limit})
         (when-let [retry-limit (not-empty (.attr chunk "retry-limit"))]
           {:chunk/retry-limit retry-limit})
         (when-let [item-reader (first (. chunk select "> reader[ref]"))]
           {:chunk/reader {:reader/ref (.attr item-reader "ref")}})
         (when-let [item-processor (first (. chunk select "> processor[ref]"))]
           {:chunk/processor {:processor/ref (.attr item-processor "ref")}})
         (when-let [item-writer (first (. chunk select "> writer[ref]"))]
           {:chunk/writer {:writer/ref (.attr item-writer "ref")}})))

(defn xml->step [step]
  (merge {}
         (when-let [id (.attr step "id")]
           {:step/name id})
         (when-let [start-limit (not-empty (.attr step "start-limit"))]
           {:step/start-limit start-limit})
         (when-let [allow-start-if-complete (not-empty (.attr step "allow-start-if-complete"))]
           {:step/allow-start-if-complete? allow-start-if-complete})
         (when-let [next (not-empty (.attr step "next"))]
           {:step/next next})
         (when-let [chunk (first (. step select "> chunk"))]
           {:step/chunk (xml->chunk chunk)})
         (when-let [batchlet (first (. step select "> batchlet"))]
           {:step/batchlet (xml->batchlet batchlet)})))

(defn xml->flow [flow]
  (merge {}
         (when-let [id (.attr flow "id")]
           {:flow/name id})
         (when-let [next (not-empty (.attr flow "next"))]
           {:flow/next next})
         (when-let [components (not-empty (.select flow "> step"))]
           {:flow/components (xml->components components)})))

(defn xml->split [split]
  (merge {}
         (when-let [id (.attr split "id")]
           {:split/name id})
         (when-let [split (not-empty (.attr split "next"))]
           {:split/next next})
         (when-let [components (not-empty (.select split "> flow"))]
           {:split/components (xml->components components)})))

(defn xml->components [job]
  (->> (for [component (. job select "> step,flow,split,decision")]
         (case (.tagName component)
           "step" (xml->step component)
           "flow" (xml->flow component)
           "split" (xml->split component)
           "decision" (throw (Exception. "Unsupported `decision`"))))
       vec))

(defn xml->edn
  "Convert a format from XML to edn."
  [xml]
  (let [doc (Jsoup/parse xml)
        job (. doc select "job")]
    {:job/name (.attr job "id")
     :job/components (xml->components job)}))

(defn- body-as-string [ctx]
  (if-let [body (get-in ctx [:request :body])]
    (condp instance? body
      java.lang.String body
      (slurp (io/reader body)))))

(defn parse-body [context]
  (when (#{:put :post} (get-in context [:request :request-method]))
    (try
      (if-let [body (body-as-string context)]
        (case (get-in context [:request :content-type])
          "application/edn" [false {:edn (edn/read-string body)}]
          "application/xml" [false {:edn (xml->edn body)}]
          false)
        false)
      (catch Exception e
        (log/error e "fail to parse edn.")
        {:message (format "IOException: %s" (.getMessage e))}))))
