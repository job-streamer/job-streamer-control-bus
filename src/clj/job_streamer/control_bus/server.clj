(ns job-streamer.control-bus.server
  (:require [ring.util.servlet :as servlet])
  (:import [net.unit8.wscl ClassProvider]
           [net.unit8.logback.server WebSocketReceiver]
           [org.xnio ByteBufferSlicePool]
           [io.undertow Undertow Handlers]
           [io.undertow.servlet Servlets]
           [io.undertow.servlet.api DeploymentInfo]
           [io.undertow.servlet.util ImmediateInstanceFactory]
           [io.undertow.websockets WebSocketConnectionCallback]
           [io.undertow.websockets.core WebSockets AbstractReceiveListener]
           [io.undertow.websockets.jsr WebSocketDeploymentInfo]))

(defn- websocket-log-receiver []
  (.. (DeploymentInfo.)
      (setClassLoader (.getContextClassLoader (Thread/currentThread)))
      (setContextPath "")
      (addServletContextAttribute
       WebSocketDeploymentInfo/ATTRIBUTE_NAME
       (.. (WebSocketDeploymentInfo.)
           (setBuffers (ByteBufferSlicePool. (int 100) (int 1000)))
           (addEndpoint WebSocketReceiver)))
      (setDeploymentName "WebSocketReceiver")))

(defn- websocket-classloader-provider []
  (.. (DeploymentInfo.)
      (setClassLoader (.getContextClassLoader (Thread/currentThread)))
      (setContextPath "")
      (addServletContextAttribute
       WebSocketDeploymentInfo/ATTRIBUTE_NAME
       (.. (WebSocketDeploymentInfo.)
           (setBuffers (ByteBufferSlicePool. (int 100) (int 1000)))
           (addEndpoint ClassProvider)))
      (setDeploymentName "WebSocketClassProvider")))

(defn send! [channel message]
  (cond
    (= (class message) String) (.sendText WebSockets channel message nil)
    :default (throw (UnsupportedOperationException. (class message)))))

(defn websocket-callback [{:keys [on-close on-message]}]
  (proxy [WebSocketConnectionCallback] []
    (onConnect [exchange channel]
      (.. channel
          getReceiveSetter
          (set (proxy [AbstractReceiveListener] []
                 (onFullTextMessage
                   [channel message]
                   (when on-message (on-message channel (.getData message))))
                 (onFullCloseMessage
                   [channel message]
                   (when on-close (on-close channel message))))))
      (.resumeReceives channel))))

(defn run-server [ring-handler & {port :port websockets :websockets}]
  (let [ring-servlet (servlet/servlet ring-handler)
        servlet-builder (.. (Servlets/deployment)
                            (setClassLoader (.getContextClassLoader (Thread/currentThread)))
                            (setContextPath "")
                            (setDeploymentName "control-bus")
                            (addServlets
                             (into-array
                              [(.. (Servlets/servlet "Ring handler"
                                                     (class ring-servlet)
                                                     (ImmediateInstanceFactory. ring-servlet))
                                   (addMapping "/*"))])))
        container (Servlets/defaultContainer)
        servlet-manager (.addDeployment container servlet-builder)
        wscl-manager    (.addDeployment container (websocket-classloader-provider))
        wslog-manager   (.addDeployment container (websocket-log-receiver))
        handler (Handlers/path)]
    ;; deploy
    (.deploy servlet-manager)
    (.deploy wscl-manager)
    (.deploy wslog-manager)

    (doseq [ws websockets]
      (.addPrefixPath handler
                      (:path ws)
                      (Handlers/websocket
                       (websocket-callback (dissoc ws :path))))) 
    (let [server (.. (Undertow/builder)
                     (addHttpListener port "0.0.0.0")
                     (setHandler (.addPrefixPath handler "/" (.start servlet-manager)))
                     (setHandler (.addPrefixPath handler "/wscl"  (.start wscl-manager)))
                     (setHandler (.addPrefixPath handler "/wslog" (.start wslog-manager)))
                     (build))]
      (.start server)
      server)))
