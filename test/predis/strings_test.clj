(ns predis.strings-test
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            (clojure.test.check
              [clojure-test :refer [defspec]]
              [generators :as gen]
              [properties :as prop])
            [taoensso.carmine :as carmine]
            (predis
              [core :as r]
              [mock :as mock])
            [predis.test-utils :as test-utils]))

(def carmine-client (test-utils/carmine-client))

(use-fixtures :each test-utils/flush-redis)

(defspec test-append
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   s1 (gen/not-empty gen/string-alphanumeric)
                   s2 (gen/not-empty gen/string-alphanumeric)]
      (test-utils/assert-set mock-client carmine-client k s1)
      (is (= (r/append mock-client k s2) (r/append carmine-client k s2)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-decrby
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k (gen/not-empty gen/string-alphanumeric)
                   v gen/int
                   decrement gen/int]
      (test-utils/assert-set mock-client carmine-client k v)
      (is (= (r/decrby mock-client k decrement) (r/decrby carmine-client k decrement)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-get-set
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   v gen/string-alphanumeric]
      (test-utils/assert-set mock-client carmine-client k v)
      (is (= (r/get mock-client k)
             (r/get carmine-client k))))))

(defspec test-getset
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   v gen/string-alphanumeric]
      (test-utils/assert-set mock-client carmine-client k v)
      (is (= (r/getset mock-client k v)
             (r/getset carmine-client k v)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-getrange
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   s gen/string-alphanumeric
                   start gen/int
                   end gen/int]
      (test-utils/assert-set mock-client carmine-client k s)
      (is (= (r/getrange mock-client k start end)
             (r/getrange carmine-client k start end)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-incrby
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k (gen/not-empty gen/string-alphanumeric)
                   v gen/int
                   increment gen/int]
      (test-utils/assert-set mock-client carmine-client k v)
      (is (= (r/incrby mock-client k increment) (r/incrby carmine-client k increment)))
      (test-utils/dbs-equal mock-client carmine-client))))

;(defspec test-mget)

;(defspec test-mset)

(defspec test-strlen
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   s (gen/not-empty gen/string-alphanumeric)]
      (test-utils/assert-set mock-client carmine-client k s)
      (is (= (r/strlen mock-client k) (r/strlen carmine-client k)))
      (test-utils/dbs-equal mock-client carmine-client))))
