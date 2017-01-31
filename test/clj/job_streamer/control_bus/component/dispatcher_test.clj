(ns job-streamer.control-bus.component.dispatcher-test
  (:require (job-streamer.control-bus.component [dispatcher :as dispatcher])
            [clojure.test :refer :all]))

(deftest convert-to-test-job
  (testing "replace mock"
    (is (= (dispatcher/convert-to-test-job "<job id=\"2\">
                                <step id=\"1\">
                                <next on=\"*\" to=\"2\"></next>
                                <listeners></listeners>
                                <batchlet ref=\"example.HelloBatch\"></batchlet>
                                </step>
                                <step id=\"2\">
                                <batchlet ref=\"example.HelloBatch\"></batchlet>
                                </step>
                                </job>")
           "<job id=\"2\"> \n <step id=\"1\"> \n  <next on=\"*\" to=\"2\"></next> \n  <listeners></listeners>  \n  <batchlet ref=\"org.jobstreamer.batch.TestBatchlet\"></batchlet>\n </step> \n <step id=\"2\">  \n  <batchlet ref=\"org.jobstreamer.batch.TestBatchlet\"></batchlet>\n </step> \n</job>"))))
