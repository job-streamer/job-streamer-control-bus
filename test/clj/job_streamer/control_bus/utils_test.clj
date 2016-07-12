(ns job-streamer.control-bus.utils-test
  (:require [job-streamer.control-bus.util :refer :all]
            [clojure.test :refer :all]))

(deftest parse-job
  (let [jobxml1 "
<?xml version='1.0' encoding='utf-8'?>
<job id='job1'>
  <step id='s1'>
    <batchlet ref='Batchlet1'>
  </step>
</job>
"
        jobxml2 "
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
"]
    (testing "simple-batchlet"
      (let [job (xml->edn jobxml1)]
        (is "Batchlet1" (get-in job [:job/components :step/batchlet :batchlet/ref]))))
    (testing "simple-chunk"
      (let [job (xml->edn jobxml1)]
        (is "Processor1" (get-in job [:job/components :step/chunk :chunk/processor :proccessor/ref]))))))
