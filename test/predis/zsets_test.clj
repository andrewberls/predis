(ns predis.zsets-test
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

(defspec test-zadd-one
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   score gen/int
                   m (gen/not-empty gen/string-alphanumeric)]
      (is (= (r/zadd mock-client k score m) (r/zadd carmine-client k score m)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-zadd-many
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs test-utils/gen-kvs-vec]
      (is (= (r/zadd mock-client k kvs) (r/zadd carmine-client k kvs)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-zcard
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs test-utils/gen-kvs-vec]
      (test-utils/assert-zadd mock-client carmine-client k kvs)
      (is (= (r/zcard mock-client k) (r/zcard carmine-client k)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-zcount
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs test-utils/gen-kvs-vec

                   min-score gen/int
                   max-score-incr gen/s-pos-int]
      (let [max-score (+ min-score max-score-incr)]
        (test-utils/assert-zadd mock-client carmine-client k kvs)
        (is (= (r/zcount mock-client k min-score max-score)
               (r/zcount carmine-client k min-score max-score)))
        (test-utils/dbs-equal mock-client carmine-client)))))

(defspec test-zincrby
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs test-utils/gen-kvs-vec

                   increment gen/int]
      (test-utils/assert-zadd mock-client carmine-client k kvs)
      (let [m (second (first (shuffle kvs)))]
        (is (= (r/zincrby mock-client k increment m)
               (r/zincrby carmine-client k increment m)))
        (test-utils/dbs-equal mock-client carmine-client)))))

;;(zinterstore [this dest numkeys ks weights])
;;(zlexcount [this k min-val max-val])

(defspec test-zrange
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs test-utils/gen-kvs-vec

                   min-score gen/int
                   max-score-incr gen/s-pos-int]
      (test-utils/assert-zadd mock-client carmine-client k kvs)
      (let [max-score (+ min-score max-score-incr)]
        (is (= (r/zrange mock-client k min-score max-score)
               (r/zrange carmine-client k min-score max-score)))
        (is (= (r/zrange mock-client k min-score max-score {:withscores true})
               (r/zrange carmine-client k min-score max-score {:withscores true})))
        (test-utils/dbs-equal mock-client carmine-client)))))

;;(zrangebylex [this k min-val max-val opts?])

(defspec test-zrangebyscore
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs test-utils/gen-kvs-vec

                   min-score gen/int
                   max-score-incr gen/s-pos-int]
      (let [max-score (+ min-score max-score-incr)]
        (test-utils/assert-zadd mock-client carmine-client k kvs)
        (is (= (r/zrangebyscore mock-client k min-score max-score)
               (r/zrangebyscore carmine-client k min-score max-score)))

        (is (= (r/zrangebyscore mock-client k min-score max-score {:withscores true})
               (r/zrangebyscore carmine-client k min-score max-score {:withscores true})))

        ; TODO: limit, offset

        (test-utils/dbs-equal mock-client carmine-client)))))

(defspec test-zrank
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs test-utils/gen-kvs-vec]
      (test-utils/assert-zadd mock-client carmine-client k kvs)
      (let [m (second (first (shuffle kvs)))]
        (is (= (r/zrank mock-client k m)
               (r/zrank carmine-client k m)))
        (test-utils/dbs-equal mock-client carmine-client)))))

;(zrem [this k m-or-ms])
;;(zremrangebylex [this k min-val max-val])
;(zremrangebyscore [this k min-score max-score])
;(zrevrange [this k start stop] [this k start stop opts])
;(zrevrangebyscore [this k max-score min-score opts])
;(zrevrank [this k m])
;(zscore [this k m])
;(zunionstore [dest numkeys ks weights])
;;(zscan [this k cursor] [this k cursor opts])
