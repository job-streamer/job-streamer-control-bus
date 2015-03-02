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
          (recur (conj addresses {:host (InetAddress/getByAddress ip-bytes)
                                  :port port})))))))

(defn- available-addresses []
  (flatten
   (for [interface (enumeration-seq (NetworkInterface/getNetworkInterfaces))]
    (->> (.getInterfaceAddresses interface) 
         (map #(.getAddress %))
         (filter #(and (instance? java.net.Inet4Address %)
                       (not (.isLoopbackAddress %))))))))

(defn- select-control-bus-url [agent-addr]
  (let [segments (.getAddress (:host agent-addr))]
    (loop [i 0, addrs (available-addresses)]
      (let [matches (filter #(= (aget segments i)
                                (aget (.getAddress %) i)) addrs)]
        (cond (empty? matches) (or (first addrs) (InetAddress/getLocalHost)) 
              (or (= (count matches) 1) (= i 3)) (first matches)
              :default (recur (inc i) matches))))))

(defn- do-receive [key ws-port]
  (let [channel (.channel key)
        buf (ByteBuffer/allocate 256)]
    (.receive channel buf)
    (.flip buf)
    
    (let [agent-address (first (read-agent-addresses buf))]
      (log/debug "Find agent: " agent-address)
      (log/debug "Send join request" (str "http://" (.getHostAddress (:host agent-address))
                       ":" (:port agent-address) "/join-bus"))
      (log/debug "  :control-bus-url " (str "ws://" (.getHostAddress (select-control-bus-url agent-address))
                        ":" ws-port "/join"))
      (log/debug "  :agent-host " (.getHostAddress (:host agent-address)))
      @(http/post (str "http://" (.getHostAddress (:host agent-address))
                       ":" (:port agent-address) "/join-bus")
                  {:form-params
                   {:control-bus-url (str "ws://" (.getHostAddress (select-control-bus-url agent-address))
                                          ":" ws-port "/join")
                    :agent-host (.getHostAddress (:host agent-address))}}
                  (fn [{:keys [status headers error]}]
                    (log/warn "join-request" status))))))

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

