(ns dev
  (:refer-clojure :exclude [test])
  (:require [clojure.repl :refer :all]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.namespace.repl :refer [refresh]]
            [clojure.java.io :as io]
            [com.stuartsierra.component :as component]
            [duct.generate :as gen]
            [meta-merge.core :refer [meta-merge]]
            [reloaded.repl :refer [system init start stop go reset]]
            [ring.middleware.stacktrace :refer [wrap-stacktrace]]
            [dev.tasks :refer :all]
            [job-streamer.control-bus.config :as config]
            [job-streamer.control-bus.system :as system]))

(def dev-config
  {:app {:middleware [wrap-stacktrace]}
   :datomic {:uri "datomic:mem://jobstreamer"}})

(def config
  (meta-merge config/defaults
              config/environ
              dev-config))

(defn new-system []
  (into (system/new-system config)
        {}))

(when (io/resource "local.clj")
  (load "local"))

(gen/set-ns-prefix 'duct-test)

(reloaded.repl/set-init! new-system)

