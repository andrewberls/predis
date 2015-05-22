(ns predis.sets-test
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

(defspec test-sadd
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k1 gen/string-alphanumeric
                   v gen/int
                   k2 gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (is (= (r/sadd mock-client k1 v) (r/sadd carmine-client k1 v)))
      (is (= (r/sadd mock-client k2 vs) (r/sadd carmine-client k2 vs)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-scard
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (test-utils/assert-sadd mock-client carmine-client k vs)
      (is (= (r/scard mock-client k) (r/scard carmine-client k)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-sdiff
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k1 gen/string-alphanumeric
                   vs1 (gen/not-empty (gen/vector gen/int))

                   k2 gen/string-alphanumeric
                   vs2 (gen/not-empty (gen/vector gen/int))]
      (test-utils/assert-sadd mock-client carmine-client k1 vs1)
      (test-utils/assert-sadd mock-client carmine-client k2 vs2)
      (is (= (sort (r/sdiff mock-client k1)) (sort (r/sdiff carmine-client k1))))
      (is (= (sort (r/sdiff mock-client [k1 k2])) (sort (r/sdiff carmine-client [k1 k2]))))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-sdiffstore
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [src gen/string-alphanumeric
                   dest gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (test-utils/assert-sadd mock-client carmine-client src vs)
      (is (= (r/sdiffstore mock-client dest src) (r/sdiffstore carmine-client dest src)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-sinter
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k1 gen/string-alphanumeric
                   vs1 (gen/not-empty (gen/vector gen/int))

                   k2 gen/string-alphanumeric
                   vs2 (gen/not-empty (gen/vector gen/int))]
      (test-utils/assert-sadd mock-client carmine-client k1 vs1)
      (test-utils/assert-sadd mock-client carmine-client k2 vs2)
      (is (= (sort (r/sinter mock-client k1)) (sort (r/sinter carmine-client k1))))
      (is (= (sort (r/sinter mock-client [k1 k2])) (sort (r/sinter carmine-client [k1 k2]))))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-sinterstore
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [src gen/string-alphanumeric
                   dest gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (test-utils/assert-sadd mock-client carmine-client src vs)
      (is (= (r/sinterstore mock-client dest src) (r/sinterstore carmine-client dest src)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-sismember
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (test-utils/assert-sadd mock-client carmine-client k vs)
      (let [v (first vs)]
        (is (= (r/sismember mock-client k v) (r/sismember carmine-client k v))))
      (is (= (r/sismember mock-client k "fake") (r/sismember carmine-client k "fake")))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-smembers
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (test-utils/assert-sadd mock-client carmine-client k vs)
      (is (= (sort (r/smembers mock-client k)) (sort (r/smembers carmine-client k))))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-smove
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k1 gen/string-alphanumeric
                   k2 gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))

                   fake-1 gen/string-alphanumeric
                   fake-2 gen/string-alphanumeric
                   ]
      (test-utils/assert-sadd mock-client carmine-client k1 vs)
      (let [v (first vs)]
        (is (= (r/smove mock-client k1 k2 v) (r/smove mock-client k1 k2 v)))
        (is (= (r/smove mock-client k1 k2 "fake") (r/smove mock-client k1 k2 "fake")))
        (is (= (r/smove mock-client fake-1 k2 v) (r/smove mock-client fake-1 k2 v)))
        (is (= (r/smove mock-client k1 fake-2 v) (r/smove mock-client k1 fake-2 v)))
      (test-utils/dbs-equal mock-client carmine-client)))))

(defspec test-srem
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (test-utils/assert-sadd mock-client carmine-client k vs)
      (let [v (first vs)]
        (is (= (r/srem mock-client k v) (r/srem carmine-client k v)))
        (is (= (r/srem mock-client k "fake") (r/srem carmine-client k "fake"))))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-sunion
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k1 gen/string-alphanumeric
                   vs1 (gen/not-empty (gen/vector gen/int))

                   k2 gen/string-alphanumeric
                   vs2 (gen/not-empty (gen/vector gen/int))]
      (test-utils/assert-sadd mock-client carmine-client k1 vs1)
      (test-utils/assert-sadd mock-client carmine-client k2 vs2)
      (is (= (sort (r/sunion mock-client k1)) (sort (r/sunion carmine-client k1))))
      (is (= (sort (r/sunion mock-client [k1 k2])) (sort (r/sunion carmine-client [k1 k2]))))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-sunionstore
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [src gen/string-alphanumeric
                   dest gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (test-utils/assert-sadd mock-client carmine-client src vs)
      (is (= (r/sunionstore mock-client dest src) (r/sunionstore carmine-client dest src)))
      (test-utils/dbs-equal mock-client carmine-client))))
