(ns job-streamer.control-bus.multicast
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [org.httpkit.client :as http])
  (:use [environ.core :only [env]])
  (:import [java.nio.channels DatagramChannel Selector SelectionKey]
           [java.nio ByteBuffer]
           [java.io ByteArrayInputStream DataInputStream]
           [java.net InetSocketAddress InetAddress]))

(defn- create-channel [port]
  (let [channel (DatagramChannel/open)]
    (doto (.socket channel)
      (.setBroadcast true)
      (.setReuseAddress true)
      (.bind (InetSocketAddress. port)))
    (.configureBlocking channel false)
    channel))

(defn- read-hostname [buf]
  (let [dis (DataInputStream. (ByteArrayInputStream. (.array buf)))
        ip-bytes (make-array Byte/TYPE 4)]
    (.read dis ip-bytes 0 4)
    (str (.getHostAddress (InetAddress/getByAddress ip-bytes)) ":" (.readInt dis))))

(defn- do-receive [key ws-port]
  (let [channel (.channel key)
        buf (ByteBuffer/allocate 16)]
    (.receive channel buf)
    (.flip buf)
    (let [remote-host (read-hostname buf)]
      (log/info "Find agent: " remote-host)
      @(http/post (str "http://" remote-host "/join-bus")
                  {:form-params
                   {:control-bus-url (str "ws://" (.getHostAddress (InetAddress/getLocalHost)) ":" ws-port "/join")}}))))

(defn start [ws-port]
  (future
    (let [channel (create-channel (or (env :control-bus-port) 45100))
          selector (Selector/open)]
      (.register channel selector SelectionKey/OP_READ)
      (loop [key-num (.select selector)]
        (when (> key-num 0)
          (let [key-set (.selectedKeys selector)]
            (doseq [key key-set]
              (.remove key-set key)
              (when (.isReadable key)
                (do-receive key ws-port)))
            (recur (.select selector))))))))

