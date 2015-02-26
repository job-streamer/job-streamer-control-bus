(ns job-streamer.control-bus.validation
  (:require [bouncer.core :as b]
            [bouncer.validators :as v])
  (:use [clojure.walk :only [postwalk]]))

(defn validate [m & forms]
  (if (sequential? m)
    (if-let [data (:edn (second m))] 
      (if-let [errors (first
                       (apply b/validate data forms))]
        {:message (->> errors
                       (postwalk #(if (map? %) (vals %) %))
                       flatten
                       first)}
        m)
      m)
    m))
