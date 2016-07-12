(ns job-streamer.control-bus.validation
  (:require [bouncer.core :as b]
            [bouncer.validators :as v]
            [clojure.walk :refer [postwalk]]))

(defn validate [m & forms]
  (if (sequential? m)
    (if-let [data (:edn (second m))]
      (if-let [errors (first
                       (apply b/validate data forms))]
        {:message {:messages (->> errors
                       (postwalk #(if (map? %) (vals %) %))
                       flatten
                       vec)}
         :representation {:media-type "application/edn"}}
        m)
      m)
    m))
