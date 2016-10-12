(ns job-streamer.control-bus.system
  (:require [com.stuartsierra.component :as component]
            [duct.component.endpoint :refer [endpoint-component]]
            [duct.component.handler :refer [handler-component]]
            [duct.middleware.not-found :refer [wrap-not-found]]
            [meta-merge.core :refer [meta-merge]]
            [ring.middleware.multipart-params :refer [wrap-multipart-params]]
            [ring.middleware.defaults :refer [wrap-defaults api-defaults]]
            [ring.util.response :refer [header]]
            (job-streamer.control-bus.component
             [undertow   :refer [undertow-server]]
             [jobs       :refer [jobs-component]]
             [agents     :refer [agents-component]]
             [calendar   :refer [calendar-component]]
             [apps       :refer [apps-component]]
             [scheduler  :refer [scheduler-component]]
             [dispatcher :refer [dispatcher-component]]
             [discoverer :refer [discoverer-component]]
             [recoverer :refer [recoverer-component]]
             [datomic    :refer [datomic-component]]
             [migration  :refer [migration-component]]
             [socketapp  :refer [socketapp-component]])
            (job-streamer.control-bus.endpoint
             [api :refer [api-endpoint]])
            [job-streamer.control-bus.endpoint.api :refer [api-endpoint]]))

(defn wrap-same-origin-policy [handler alias]
  (fn [req]
    (if (= (:request-method req) :options)
      ;;Pre-flight request
      {:status 200
       :headers {"Access-Control-Allow-Methods" "POST,GET,PUT,DELETE,OPTIONS"
                 "Access-Control-Allow-Origin" "*"
                 "Access-Control-Allow-Headers" "Content-Type,Cache-Control,X-Requested-With"}}
      (when-let [resp (handler req)]
        (header resp "Access-Control-Allow-Origin" "*")))))

(def base-config
  {:app {:middleware [[wrap-not-found :not-found]
                      [wrap-same-origin-policy :same-origin]
                      [wrap-multipart-params]
                      [wrap-defaults :defaults]]

         :not-found  "Resource Not Found"
         :defaults  (meta-merge api-defaults {})}})


(defn new-system [config]
  (let [config (meta-merge base-config config)]
    (-> (component/system-map
         :app        (handler-component    (:app config))
         :api        (endpoint-component   api-endpoint)
         :socketapp  (socketapp-component  (:socketapp  config))
         :http       (undertow-server      (:http config))
         :dispatcher (dispatcher-component (:dispatcher config))
         :discoverer (discoverer-component (:discoverer config))
         :recoverer  (recoverer-component  (:recoverer  config))
         :scheduler  (scheduler-component  (:scheduler  config))
         :datomic    (datomic-component    (:datomic    config))
         :migration  (migration-component  (:migration  config))
         :apps       (apps-component       (:apps       config))
         :jobs       (jobs-component       (:jobs       config))
         :agents     (agents-component     (:agents     config))
         :calendar   (calendar-component   (:calendar   config)))
        (component/system-using
         {:http      [:app :socketapp]
          :app       [:api]
          :api       [:apps :calendar :agents :jobs :scheduler]
          :socketapp [:datomic :jobs :agents]
          :jobs      [:datomic :scheduler :agents]
          :agents    [:datomic]
          :apps      [:datomic :agents]
          :calendar  [:datomic :scheduler]
          :scheduler [:datomic]
          :migration [:datomic]
          :recoverer [:datomic :jobs :agents]
          :dispatcher [:datomic :apps :jobs :agents]}))))
