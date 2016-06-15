(ns job-test
  (:require [clojure.test :refer :all]
            [job-streamer.control-bus.job :as job]))

(def jobxml1 "
<?xml version='1.0' encoding='utf-8'?>
<job id='job1'>
  <step id='s1'>
    <batchlet ref='Batchlet1'>
  </step>
</job>
")

(deftest simple-batchlet
  (let [job (job/xml->edn jobxml1)]
    (is "Batchlet1" (get-in job [:job/components :step/batchlet :batchlet/ref]))))


(def jobxml2 "
<?xml version='1.0' encoding='utf-8'?>
<job id='job1'>
  <step id='s1'>
    <chunk>
      <reader ref='Reader1'/>
      <processor ref='Processor1'/>
      <wirter ref='Writer1'/>
    </chunk>
  </step>
</job>
")

(deftest simple-chunk
  (let [job (job/xml->edn jobxml1)]
    (is "Processor1" (get-in job [:job/components :step/chunk :chunk/processor :proccessor/ref]))))

