(ns predis.integration-test
  ^:integration-tests
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            (clojure.test.check
              [clojure-test :refer [defspec]]
              [generators :as gen]
              [properties :as prop])
            [taoensso.carmine :as carmine]
            (predis
              [core :as r]
              [carmine :as predis.carmine]
              [mock :as mock])))

(def redis-config
  {:pool {}
   :spec {:host "127.0.0.1" :port 6379}})

(def carmine-client (predis.carmine/->redis redis-config))

(use-fixtures :each
  (fn [f]
    (carmine/wcar redis-config (carmine/flushdb))
    (f)))

(defn get-any [client k]
  (condp = (r/type client k)
    "string" (r/get client k)
    "list" (r/lrange client k 0 -1)
    "hash" (r/hgetall client k)
    "set" (r/smembers client k)
    "zset" (throw (Exception. "Unsupported type"))
    "none" nil))

(defn db
  "Return all keys and vals from client as a map"
  [client]
  (let [ks (r/keys client "*")]
    (if (seq ks)
      (let [vs (map (partial get-any client) ks)]
        (into {} (map vector ks vs)))
      {})))

(defn assert-set [c1 c2 k v]
  (is (= (r/set c1 k v) (r/set c2 k v))))

(defn assert-rpush [c1 c2 k vs]
  (is (= (r/rpush c1 k vs) (r/rpush c2 k vs)))
  (Thread/sleep 1000))

(defn dbs-equal [c1 c2]
  (is (= (db c1) (db c2))))

;;

; Keys
(defspec test-del
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   v gen/string-alphanumeric]
      (assert-set mock-client carmine-client k v)
      (dbs-equal mock-client carmine-client)
      (is (= (r/del mock-client k) (r/del carmine-client k)))
      (dbs-equal mock-client carmine-client))))

(defspec test-rename
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k1 (gen/not-empty gen/string-alphanumeric)
                   v gen/string-alphanumeric
                   k2 (gen/not-empty gen/string-alphanumeric)]
      (assert-set mock-client carmine-client k1 v)
      (is (= (r/rename mock-client k1 k2) (r/rename carmine-client k1 k2)))
      (dbs-equal mock-client carmine-client))))

; Strings
(defspec test-append
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   s1 (gen/not-empty gen/string-alphanumeric)
                   s2 (gen/not-empty gen/string-alphanumeric)]
      (assert-set mock-client carmine-client k s1)
      (is (= (r/append mock-client k s2) (r/append carmine-client k s2)))
      (dbs-equal mock-client carmine-client))))

(defspec test-decrby
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k (gen/not-empty gen/string-alphanumeric)
                   v gen/int
                   decrement gen/int]
      (assert-set mock-client carmine-client k v)
      (is (= (r/decrby mock-client k decrement) (r/decrby carmine-client k decrement)))
      (dbs-equal mock-client carmine-client))))

(defspec test-get-set
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   v gen/string-alphanumeric]
      (assert-set mock-client carmine-client k v)
      (is (= (r/get mock-client k)
             (r/get carmine-client k))))))

(defspec test-incrby
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k (gen/not-empty gen/string-alphanumeric)
                   v gen/int
                   increment gen/int]
      (assert-set mock-client carmine-client k v)
      (is (= (r/incrby mock-client k increment) (r/incrby carmine-client k increment)))
      (dbs-equal mock-client carmine-client))))

;(defspec test-mget)

;(defspec test-mset)

(defspec test-strlen
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   s (gen/not-empty gen/string-alphanumeric)]
      (assert-set mock-client carmine-client k s)
      (is (= (r/strlen mock-client k) (r/strlen carmine-client k)))
      (dbs-equal mock-client carmine-client))))

; Hashes
;(defspec test-hdel)
;(defspec test-hexists)
;(defspec test-hget)
;(defspec test-hgetall)
;(defspec test-hincrby)
;(defspec test-hincrbyfloat)
;;defspec test-hkeys)
;(defspec test-hlen)
;(defspec test-hmget)
;(defspec test-hmset)
;;defspec test-hset)
;;defspec test-hsetnx)
;;defspec test-hstrlen)
;(defspec test-hvals)

; Lists
;;(lindex [this k idx])
;;(linsert [k pos pivot v])

(defspec test-llen
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (assert-rpush mock-client carmine-client k vs)
      (is (= (r/llen mock-client k) (r/llen carmine-client k)))
      (dbs-equal mock-client carmine-client))))

(defspec test-lpop
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   fake-key gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (assert-rpush mock-client carmine-client k vs)
      (is (= (r/lpop mock-client k) (r/lpop carmine-client k)))
      (is (= (r/lpop mock-client fake-key)
             (r/lpop carmine-client fake-key)))
      (dbs-equal mock-client carmine-client))))

(defspec test-lpush
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (is (= (r/lpush mock-client k vs) (r/lpush carmine-client k vs)))
      (dbs-equal mock-client carmine-client))))

(defspec test-lrange
  50
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))
                   start gen/int
                   stop gen/int]
      (assert-rpush mock-client carmine-client k vs)
      (is (= (r/lrange mock-client k start stop)
             (r/lrange carmine-client k start stop)))
      (dbs-equal mock-client carmine-client))))

;(defspec test-lrem)
;;(defspec test-lset)
;;(defspec test-ltrim)
;(defspec test-rpop)

(defspec test-rpush
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (is (= (r/rpush mock-client k vs) (r/rpush carmine-client k vs)))
      (dbs-equal mock-client carmine-client))))

; Sets
;(defspec test-sadd)
;(defspec test-scard)
;(defspec test-sdiff)
;(defspec test-sdiffstore)
;(defspec test-sinter)
;(defspec test-sinterstore)
;(defspec test-sismember)
;(defspec test-smembers)
;(defspec test-smove)
;(defspec test-spop)
;(defspec test-srandmember)
;(defspec test-srem)
;(defspec test-sunion)
;(defspec test-sunionstore)
