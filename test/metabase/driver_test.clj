(ns metabase.driver-test
  (:require [expectations :refer :all]
            [metabase.driver :as driver]))

(driver/register! ::test-driver)

(defmethod driver/available? ::test-driver [_] false)

(defmethod driver/supports? [::test-driver :foreign-keys] [_] true)

;; driver-supports?

(expect true  (driver/supports? ::test-driver :foreign-keys))
(expect false (driver/supports? ::test-driver :expressions))
