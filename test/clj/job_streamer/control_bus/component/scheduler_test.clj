(ns job-streamer.control-bus.component.scheduler-test
  (:require (job-streamer.control-bus.component [scheduler :as scheduler])
            [clojure.test :refer :all]
            [clojure.pprint :refer :all]))

(deftest hh:mm?
  (testing "nil"
    (is (not (scheduler/hh:mm? nil))))
  (testing "valid"
    (is (scheduler/hh:mm? "00:00"))
    (is (scheduler/hh:mm? "23:59")))
  (testing "invalid-hour"
    (is (not (scheduler/hh:mm? "-01:01")))
    (is (not (scheduler/hh:mm? "24:01"))))
  (testing "invalid-minutes"
    (is (not (scheduler/hh:mm? "01:-01")))
    (is (not (scheduler/hh:mm? "01:60"))))
  (testing "invalid-format"
    (is (not (scheduler/hh:mm? "0101")))
    (is (not (scheduler/hh:mm? "a")))))

(deftest to-ms-from-hh:mm
  (testing "nil"
    (is (= 0 (scheduler/to-ms-from-hh:mm nil))))
  (testing "empty"
    (is (= 0 (scheduler/to-ms-from-hh:mm ""))))
  (testing "nomal"
    (is (= 3660000 (scheduler/to-ms-from-hh:mm "01:01"))))
  (testing "over8"
    (is (= 29280000 (scheduler/to-ms-from-hh:mm "08:08"))))
  (testing "invalid"
    (is (= 0 (scheduler/to-ms-from-hh:mm "this is invalid"))))
  ;This function does not have Responsibility of validation
  )
