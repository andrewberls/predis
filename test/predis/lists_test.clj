(ns predis.lists-test
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

(defspec test-lindex
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))
                   idx gen/int]
      (test-utils/assert-rpush mock-client carmine-client k vs)
      (is (= (r/lindex mock-client k idx) (r/lindex carmine-client k idx)))
      (test-utils/dbs-equal mock-client carmine-client))))

;(defspec test-linsert)

(defspec test-llen
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (test-utils/assert-rpush mock-client carmine-client k vs)
      (is (= (r/llen mock-client k) (r/llen carmine-client k)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-lpop
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   fake-key gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (test-utils/assert-rpush mock-client carmine-client k vs)
      (is (= (r/lpop mock-client k) (r/lpop carmine-client k)))
      (is (= (r/lpop mock-client fake-key)
             (r/lpop carmine-client fake-key)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-lpush
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (is (= (r/lpush mock-client k vs) (r/lpush carmine-client k vs)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-lpushx
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k1 gen/string-alphanumeric
                   k2 gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))
                   v gen/int]
      (is (= (r/lpushx mock-client k1 v) (r/lpushx carmine-client k1 v)))
      (test-utils/assert-rpush mock-client carmine-client k2 vs)
      (is (= (r/lpushx mock-client k2 v) (r/lpushx carmine-client k2 v)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-lrange
  50
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))
                   start gen/int
                   stop gen/int]
      (test-utils/assert-rpush mock-client carmine-client k vs)
      (is (= (r/lrange mock-client k start stop)
             (r/lrange carmine-client k start stop)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-lrem
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))
                   cnt gen/int]
      (test-utils/assert-rpush mock-client carmine-client k vs)
      (let [v (first (shuffle vs))]
        (is (= (r/lrem mock-client k cnt v) (r/lrem carmine-client k cnt v)))
        (test-utils/dbs-equal mock-client carmine-client)))))

;;(defspec test-lset)
;;(defspec test-ltrim)

(defspec test-rpop
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   fake-key gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (test-utils/assert-rpush mock-client carmine-client k vs)
      (is (= (r/rpop mock-client k) (r/rpop carmine-client k)))
      (is (= (r/rpop mock-client fake-key)
             (r/rpop carmine-client fake-key)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-rpush
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (is (= (r/rpush mock-client k vs) (r/rpush carmine-client k vs)))
      (test-utils/dbs-equal mock-client carmine-client))))
