(ns predis.hashes-test
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

(defspec test-hdel
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs (gen/not-empty (gen/vector test-utils/gen-hash-kv))]
      (test-utils/assert-hmset mock-client carmine-client k kvs)
      (let [f (ffirst kvs)]
        (is (= (r/hdel mock-client k f) (r/hdel carmine-client k f)))
        (is (= (r/hdel mock-client k "fake-field") (r/hdel carmine-client k "fake-field")))
        (test-utils/dbs-equal mock-client carmine-client)))))

(defspec test-hexists
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs (gen/not-empty (gen/vector test-utils/gen-hash-kv))]
      (test-utils/assert-hmset mock-client carmine-client k kvs)
      (let [f (ffirst kvs)]
        (is (= (r/hexists mock-client k f) (r/hexists carmine-client k f)))
        (is (= (r/hexists mock-client k "fake-field") (r/hexists carmine-client k "fake-field")))
        (test-utils/dbs-equal mock-client carmine-client)))))

(defspec test-hget
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs (gen/not-empty (gen/vector test-utils/gen-hash-kv))]
      (test-utils/assert-hmset mock-client carmine-client k kvs)
      (let [f (ffirst kvs)]
        (is (= (r/hget mock-client k f) (r/hget carmine-client k f)))
        (is (= (r/hget mock-client k "fake-field") (r/hget carmine-client k "fake-field")))
        (is (= (r/hget mock-client "fake-key" "fake-field")
               (r/hget carmine-client "fake-key" "fake-field")))
        (test-utils/dbs-equal mock-client carmine-client)))))

(defspec test-hgetall
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs (gen/not-empty (gen/vector test-utils/gen-hash-kv))]
      (is (= (r/hmset mock-client k kvs) (r/hmset carmine-client k kvs)))
      (is (= (sort (r/hgetall mock-client k)) (sort (r/hgetall carmine-client k))))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-hincrby
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs (gen/not-empty
                         (gen/vector (gen/tuple
                                       (gen/not-empty gen/string-alphanumeric)
                                       gen/int)))
                   increment gen/int]
      (test-utils/assert-hmset mock-client carmine-client k kvs)
      (let [f (ffirst kvs)]
        (is (= (r/hincrby mock-client k f increment)
               (r/hincrby carmine-client k f increment)))
        (is (= (r/hincrby mock-client"fake-key" f increment)
               (r/hincrby carmine-client"fake-key" f increment)))
        (is (= (r/hincrby mock-client k "fake-field" increment)
               (r/hincrby carmine-client k "fake-field" increment)))
        (test-utils/dbs-equal mock-client carmine-client)))))

(defspec test-hkeys
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs (gen/not-empty (gen/vector test-utils/gen-hash-kv))]
      (test-utils/assert-hmset mock-client carmine-client k kvs)
      (is (= (sort (r/hkeys mock-client k)) (sort (r/hkeys carmine-client k)))))))

(defspec test-hlen
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs (gen/not-empty (gen/vector test-utils/gen-hash-kv))]
      (test-utils/assert-hmset mock-client carmine-client k kvs)
      (is (= (r/hlen mock-client k) (r/hlen carmine-client k)))
      (is (= (r/hlen mock-client "fake-key") (r/hlen carmine-client "fake-key")))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-hmget
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs (gen/not-empty (gen/vector test-utils/gen-hash-kv))
                   nfields gen/s-pos-int]
      (test-utils/assert-hmset mock-client carmine-client k kvs)
      (let [fs (take nfields (map first kvs))]
        (is (= (r/hmget mock-client k fs) (r/hmget carmine-client k fs)))
        (is (= (r/hmget mock-client "fake-key" fs) (r/hmget carmine-client "fake-key" fs)))
        (test-utils/dbs-equal mock-client carmine-client)))))

(defspec test-hmset
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs (gen/not-empty (gen/vector test-utils/gen-hash-kv))]
      (is (= (r/hmset mock-client k kvs) (r/hmset carmine-client k kvs)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-hset
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   f (gen/not-empty gen/string-alphanumeric)
                   v gen/int]
      (is (= (r/hset mock-client k f v) (r/hset carmine-client k f v)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-hsetnx
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   f (gen/not-empty gen/string-alphanumeric)
                   v1 gen/int
                   v2 gen/int]
      (is (= (r/hsetnx mock-client k f v1) (r/hsetnx carmine-client k f v1)))
      (is (= (r/hsetnx mock-client k f v2) (r/hsetnx carmine-client k f v2)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-hvals
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs (gen/not-empty (gen/vector test-utils/gen-hash-kv))]
      (test-utils/assert-hmset mock-client carmine-client k kvs)
      (is (= (sort (r/hvals mock-client k)) (sort (r/hvals carmine-client k))))
      (test-utils/dbs-equal mock-client carmine-client))))
