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
  test-utils/nruns
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   s1 (gen/not-empty gen/string-alphanumeric)
                   s2 (gen/not-empty gen/string-alphanumeric)]
      (test-utils/assert-set mock-client carmine-client k s1)
      (is (= (r/append mock-client k s2) (r/append carmine-client k s2)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-decrby
  test-utils/nruns
  (let [mock-client (mock/->redis)]
    (prop/for-all [k (gen/not-empty gen/string-alphanumeric)
                   v gen/int
                   decrement gen/int]
      (test-utils/assert-set mock-client carmine-client k v)
      (is (= (r/decrby mock-client k decrement) (r/decrby carmine-client k decrement)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-get-set
  test-utils/nruns
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   v gen/string-alphanumeric]
      (test-utils/assert-set mock-client carmine-client k v)
      (is (= (r/get mock-client k)
             (r/get carmine-client k))))))

(defspec test-getset
  test-utils/nruns
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   v gen/string-alphanumeric]
      (test-utils/assert-set mock-client carmine-client k v)
      (is (= (r/getset mock-client k v)
             (r/getset carmine-client k v)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-getrange
  test-utils/nruns
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
  test-utils/nruns
  (let [mock-client (mock/->redis)]
    (prop/for-all [k (gen/not-empty gen/string-alphanumeric)
                   v gen/int
                   increment gen/int]
      (test-utils/assert-set mock-client carmine-client k v)
      (is (= (r/incrby mock-client k increment) (r/incrby carmine-client k increment)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-mget
  test-utils/nruns
  (let [mock-client (mock/->redis)]
    (prop/for-all [kvs test-utils/gen-kvs-vec]
      (is (= (r/mset mock-client kvs) (r/mset carmine-client kvs)))
      (let [num-keys (inc (rand-int (count kvs)))
            ks (take num-keys (map first kvs))]
        (is (= (r/mget mock-client ks) (r/mget carmine-client ks)))
        (test-utils/dbs-equal mock-client carmine-client)))))

(defspec test-mset
  test-utils/nruns
  (let [mock-client (mock/->redis)]
    (prop/for-all [kvs test-utils/gen-kvs-vec]
      (is (= (r/mset mock-client kvs) (r/mset carmine-client kvs)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-msetnx
  test-utils/nruns
  (let [mock-client (mock/->redis)]
    (prop/for-all [kvs test-utils/gen-kvs-vec]
      (let [existing-k (ffirst kvs)]
        (is (= (r/msetnx mock-client kvs) (r/msetnx carmine-client kvs)))
        (is (= (r/msetnx mock-client [[existing-k "foo"]])
               (r/msetnx carmine-client [[existing-k "foo"]])))
        (test-utils/dbs-equal mock-client carmine-client)))))

(defspec test-strlen
  test-utils/nruns
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   s (gen/not-empty gen/string-alphanumeric)]
      (test-utils/assert-set mock-client carmine-client k s)
      (is (= (r/strlen mock-client k) (r/strlen carmine-client k)))
      (test-utils/dbs-equal mock-client carmine-client))))
