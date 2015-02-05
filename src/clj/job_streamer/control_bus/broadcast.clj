(ns job-streamer.control-bus.broadcast
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [org.httpkit.client :as http])
  (:use [environ.core :only [env]])
  (:import [java.nio.channels DatagramChannel Selector SelectionKey]
           [java.nio ByteBuffer]
           [java.io ByteArrayInputStream DataInputStream]
           [java.net InetSocketAddress InetAddress NetworkInterface]))

(defn- create-channel [port]
  (let [channel (DatagramChannel/open)]
    (doto (.socket channel)
      (.setBroadcast true)
      (.setReuseAddress true)
      (.bind (InetSocketAddress. port)))
    (.configureBlocking channel false)
    channel))

(defn- read-agent-addresses [buf]
  (let [dis (DataInputStream. (ByteArrayInputStream. (.array buf)))
        ip-bytes (make-array Byte/TYPE 4)]
    (loop [addresses []]
      (let [_ (try (.read dis ip-bytes 0 4) (catch Exception e)) 
            port (try (.readInt dis) (catch Exception e 0))]
        (if (= port 0)
          addresses
          (recur (conj addresses (str (.getHostAddress (InetAddress/getByAddress ip-bytes)) ":" port))))))))

(defn- control-bus-address []
  (loop [i 1, addresses []]
    (if-let [interface (NetworkInterface/getByIndex i)]
      (recur (inc i)
             (concat addresses
                     (->> (.getInterfaceAddresses interface) 
                          (map #(.getAddress %))
                          (filter #(and (instance? java.net.Inet4Address %)
                                        (not (.isLoopbackAddress %)))))))
      (first addresses))))

(defn- do-receive [key ws-port]
  (let [channel (.channel key)
        buf (ByteBuffer/allocate 256)]
    (.receive channel buf)
    (.flip buf)
    (let [agent-addresses (read-agent-addresses buf)]
      (log/info "Find agent: " agent-addresses)
      @(http/post (str "http://" (first agent-addresses) "/join-bus")
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

