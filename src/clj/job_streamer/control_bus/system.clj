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
             [socketapp  :refer [socketapp-component]]
             [token      :refer [token-provider-component] :as token]
             [auth       :refer [auth-component]])
            (job-streamer.control-bus.endpoint
             [api :refer [api-endpoint]])
            [job-streamer.control-bus.endpoint.api :refer [api-endpoint]]
            [clojure.tools.logging :as log]
            [buddy.auth :refer [authenticated?]]
            [buddy.auth.backends.session :refer [session-backend]]
            [buddy.auth.backends.token :refer [token-backend]]
            [buddy.auth.middleware :refer [wrap-authentication wrap-authorization]]
            [buddy.auth.accessrules :refer [wrap-access-rules]]
            [buddy.auth.http :as http]))

(defn wrap-internal-server-error [handler]
  (fn [req]
    (try
      (handler req)
      (catch Exception e
        (log/error e)
        {:status 500
         :headers {"Access-Control-Allow-Origin" "http://localhost:3000"
                   "Access-Control-Allow-Credentials" "true"}
         :body (pr-str {:message (str "Internal server error: " (.getMessage e))})}))))

(defn wrap-same-origin-policy [handler {:keys [alias access-control-allow-origin]}]
  (fn [req]
    (if (= (:request-method req) :options)
      ;;Pre-flight request
      {:status 200
       :headers {"Access-Control-Allow-Methods" "POST,GET,PUT,DELETE,OPTIONS"
                 "Access-Control-Allow-Origin" access-control-allow-origin
                 "Access-Control-Allow-Headers" "Content-Type"
                 "Access-Control-Allow-Credentials" "true"}}
      (when-let [resp (handler req)]
        (-> resp
            (header "Access-Control-Allow-Origin" access-control-allow-origin)
            (header "Access-Control-Allow-Credentials" "true"))))))

(def access-rules [{:pattern #"^/(?!auth|user).*$"
                    :handler authenticated?}])

(defn token-base [token-provider]
  (token-backend
   {:authfn
    (fn [req token]
      (try
        (let [user (token/auth-by token-provider token)]
          (log/info "token authentication token=" token ", user=" user)
          user)
        (catch Exception e
          (log/error "auth-by error" e))))}))

(defn wrap-authn [handler token-provider & backends]
  (apply wrap-authentication handler (conj backends (token-base token-provider))))

(def base-config
  {:app {:middleware [[wrap-not-found :not-found]
                      [wrap-access-rules   :access-rules]
                      [wrap-authorization  :authorization]
                      [wrap-authn          :token :session-base]
                      [wrap-same-origin-policy :same-origin]
                      [wrap-multipart-params]
                      [wrap-internal-server-error]
                      [wrap-defaults :defaults]]
         :access-rules {:rules access-rules :policy :allow}
         :session-base (session-backend)
         :authorization (fn [req meta]
                          (if (authenticated? req)
                            (http/response "Permission denied" 403)
                            (http/response "Unauthorized" 401)))
         :not-found  "Resource Not Found"
         :defaults  (meta-merge api-defaults {:session true})}})


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
         :calendar   (calendar-component   (:calendar   config))
         :token      (token-provider-component (:token config))
         :auth       (auth-component       (:auth config)))
        (component/system-using
         {:http      [:app :socketapp]
          :app       [:api :token]
          :api       [:apps :calendar :agents :jobs :scheduler :auth]
          :socketapp [:datomic :jobs :agents]
          :jobs      [:datomic :scheduler :agents]
          :agents    [:datomic]
          :apps      [:datomic :agents]
          :calendar  [:datomic :scheduler]
          :scheduler [:datomic :token]
          :migration [:datomic]
          :recoverer [:datomic :jobs :agents]
          :dispatcher [:datomic :apps :jobs :agents]
          :auth      [:token :datomic :apps]}))))
