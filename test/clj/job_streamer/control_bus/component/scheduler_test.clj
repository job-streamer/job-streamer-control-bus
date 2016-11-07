(ns job-streamer.control-bus.component.scheduler-test
  (:require (job-streamer.control-bus.component [scheduler :as scheduler])
            [clojure.test :refer :all]
            [clojure.pprint :refer :all]))

(deftest hh:MM?
  (testing "nil"
    (is (not (scheduler/hh:MM? nil))))
  (testing "valid"
    (is (scheduler/hh:MM? "00:00"))
    (is (scheduler/hh:MM? "23:59")))
  (testing "invalid-hour"
    (is (not (scheduler/hh:MM? "-01:01")))
    (is (not (scheduler/hh:MM? "24:01"))))
  (testing "invalid-minutes"
    (is (not (scheduler/hh:MM? "01:-01")))
    (is (not (scheduler/hh:MM? "01:60"))))
  (testing "invalid-format"
    (is (not (scheduler/hh:MM? "0101")))
    (is (not (scheduler/hh:MM? "a")))))

(deftest to-ms-from-hh:MM
  (testing "nil"
    (is (= 0 (scheduler/to-ms-from-hh:MM nil))))
  (testing "empty"
    (is (= 0 (scheduler/to-ms-from-hh:MM ""))))
  (testing "nomal"
    (is (= 3660000 (scheduler/to-ms-from-hh:MM "01:01"))))
  (testing "invalid"
    (is (= 0 (scheduler/to-ms-from-hh:MM "this is invalid"))))
  ;This function does not have Responsibility of validation
  )
