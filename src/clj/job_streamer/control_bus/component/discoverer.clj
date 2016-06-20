(ns job-streamer.control-bus.component.discoverer
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.core.async :refer [go-loop close!]]
            [com.stuartsierra.component :as component]
            [environ.core :refer [env]]
            [org.httpkit.client :as http])
  (:import [java.nio.channels DatagramChannel Selector SelectionKey]
           [java.nio ByteBuffer]
           [java.io ByteArrayInputStream DataInputStream]
           [java.net InetSocketAddress InetAddress NetworkInterface StandardSocketOptions]))

(defn- create-channel [port]
  (let [channel (DatagramChannel/open)]
    (doto (.socket channel)
      (.setBroadcast true)
      (.setReuseAddress true)
      (.bind (InetSocketAddress. port)))
    (.configureBlocking channel false)
    (log/info "Listen broadcast messages from agents at " port)
    channel))

(defn- create-multicast-channel [address port]
  (let [channel (DatagramChannel/open)]
    (doto channel
      (.configureBlocking false)
      (.setOption StandardSocketOptions/SO_REUSEADDR true))
    (.. channel socket (bind (InetSocketAddress. port)))

    (doseq [interface (enumeration-seq (NetworkInterface/getNetworkInterfaces))]
      (.setOption channel StandardSocketOptions/IP_MULTICAST_IF interface)
      (.join channel address interface))
    (log/info "Listen mulicast messages from agents at " address ":" port)
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
                    (log/debug "join-request" status))))))

(defrecord Discoverer [ws-port]
  component/Lifecycle

  (start [component]
    (let [port (Integer/parseInt (or (:discovery-port env) "45100"))
          channel (if-let [address (:discovery-address env)]
                            (create-multicast-channel (InetAddress/getByName address) port)
                            (create-channel port))
          selector (Selector/open)]
      (.register channel selector SelectionKey/OP_READ)
      
      (assoc component
             :port port
             :channel  channel
             :selector selector
             :main-loop (go-loop [key-num (.select selector)]
                          (when (> key-num 0)
                            (let [key-set (.selectedKeys selector)]
                              (doseq [key key-set]
                                (.remove key-set key)
                                (when (.isReadable key)
                                  (do-receive key ws-port)))
                              (recur (.select selector))))))))

  (stop [component]
    (if-let [selector (:selector component)]
      (.close selector))
    
    (if-let [channel (:channel component)]
      (.close channel))

    (if-let [main-loop (:main-loop component)]
      (close! main-loop))

    (dissoc component :port :selector :channel :main-loop)))

(defn discoverer-component [options]
  (map->Discoverer options))
