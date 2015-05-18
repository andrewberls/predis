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

(def gen-kv
  (gen/tuple
    (gen/not-empty gen/string-alphanumeric)
    (gen/not-empty gen/string-alphanumeric)))

(defn get-any [client k]
  (condp = (r/type client k)
    "string" (r/get client k)
    "list" (r/lrange client k 0 -1)
    "hash" (sort (r/hgetall client k))
    "set" (sort (r/smembers client k))
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
  (is (= (r/rpush c1 k vs) (r/rpush c2 k vs))))

(defn assert-hmset [c1 c2 k kvs]
  (is (= (r/hmset c1 k kvs) (r/hmset c2 k kvs))))

(defn assert-sadd [c1 c2 k vs]
  (is (= (r/sadd c1 k vs) (r/sadd c2 k vs))))

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

; Server
(defspec test-dbsize
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k1 (gen/not-empty gen/string-alphanumeric)
                   v1 gen/string-alphanumeric
                   k2 (gen/not-empty gen/string-alphanumeric)
                   v2 gen/string-alphanumeric]
      (assert-set mock-client carmine-client k1 v1)
      (assert-set mock-client carmine-client k2 v2)
      (is (= (r/dbsize mock-client) (r/dbsize carmine-client))))))

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
(defspec test-hdel
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs (gen/not-empty (gen/vector gen-kv))]
      (assert-hmset mock-client carmine-client k kvs)
      (let [f (ffirst kvs)]
        (is (= (r/hdel mock-client k f) (r/hdel carmine-client k f)))
        (is (= (r/hdel mock-client k "fake-field") (r/hdel carmine-client k "fake-field")))
        (dbs-equal mock-client carmine-client)))))

(defspec test-hexists
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs (gen/not-empty (gen/vector gen-kv))]
      (assert-hmset mock-client carmine-client k kvs)
      (let [f (ffirst kvs)]
        (is (= (r/hexists mock-client k f) (r/hexists carmine-client k f)))
        (is (= (r/hexists mock-client k "fake-field") (r/hexists carmine-client k "fake-field")))
        (dbs-equal mock-client carmine-client)))))

(defspec test-hget
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs (gen/not-empty (gen/vector gen-kv))]
      (assert-hmset mock-client carmine-client k kvs)
      (let [f (ffirst kvs)]
        (is (= (r/hget mock-client k f) (r/hget carmine-client k f)))
        (is (= (r/hget mock-client k "fake-field") (r/hget carmine-client k "fake-field")))
        (is (= (r/hget mock-client "fake-key" "fake-field")
               (r/hget carmine-client "fake-key" "fake-field")))
        (dbs-equal mock-client carmine-client)))))

(defspec test-hgetall
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs (gen/not-empty (gen/vector gen-kv))]
      (is (= (r/hmset mock-client k kvs) (r/hmset carmine-client k kvs)))
      (is (= (sort (r/hgetall mock-client k)) (sort (r/hgetall carmine-client k))))
      (dbs-equal mock-client carmine-client))))

(defspec test-hincrby
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs (gen/not-empty
                         (gen/vector (gen/tuple
                                       (gen/not-empty gen/string-alphanumeric)
                                       gen/int)))
                   increment gen/int]
      (assert-hmset mock-client carmine-client k kvs)
      (let [f (ffirst kvs)]
        (is (= (r/hincrby mock-client k f increment)
               (r/hincrby carmine-client k f increment)))
        (is (= (r/hincrby mock-client"fake-key" f increment)
               (r/hincrby carmine-client"fake-key" f increment)))
        (is (= (r/hincrby mock-client k "fake-field" increment)
               (r/hincrby carmine-client k "fake-field" increment)))
        (dbs-equal mock-client carmine-client)))))

(defspec test-hkeys
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs (gen/not-empty (gen/vector gen-kv))]
      (assert-hmset mock-client carmine-client k kvs)
      (is (= (sort (r/hkeys mock-client k)) (sort (r/hkeys carmine-client k)))))))

(defspec test-hlen
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs (gen/not-empty (gen/vector gen-kv))]
      (assert-hmset mock-client carmine-client k kvs)
      (is (= (r/hlen mock-client k) (r/hlen carmine-client k)))
      (is (= (r/hlen mock-client "fake-key") (r/hlen carmine-client "fake-key")))
      (dbs-equal mock-client carmine-client))))

(defspec test-hmget
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs (gen/not-empty (gen/vector gen-kv))
                   nfields gen/s-pos-int]
      (assert-hmset mock-client carmine-client k kvs)
      (let [fs (take nfields (map first kvs))]
        (is (= (r/hmget mock-client k fs) (r/hmget carmine-client k fs)))
        (is (= (r/hmget mock-client "fake-key" fs) (r/hmget carmine-client "fake-key" fs)))
        (dbs-equal mock-client carmine-client)))))

(defspec test-hmset
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs (gen/not-empty (gen/vector gen-kv))]
      (is (= (r/hmset mock-client k kvs) (r/hmset carmine-client k kvs)))
      (dbs-equal mock-client carmine-client))))

(defspec test-hset
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   f (gen/not-empty gen/string-alphanumeric)
                   v gen/int]
      (is (= (r/hset mock-client k f v) (r/hset carmine-client k f v)))
      (dbs-equal mock-client carmine-client))))

(defspec test-hsetnx
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   f (gen/not-empty gen/string-alphanumeric)
                   v1 gen/int
                   v2 gen/int]
      (is (= (r/hsetnx mock-client k f v1) (r/hsetnx carmine-client k f v1)))
      (is (= (r/hsetnx mock-client k f v2) (r/hsetnx carmine-client k f v2)))
      (dbs-equal mock-client carmine-client))))

(defspec test-hvals
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   kvs (gen/not-empty (gen/vector gen-kv))]
      (assert-hmset mock-client carmine-client k kvs)
      (is (= (sort (r/hvals mock-client k)) (sort (r/hvals carmine-client k))))
      (dbs-equal mock-client carmine-client))))

; Lists
(defspec test-lindex
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))
                   idx gen/int]
      (assert-rpush mock-client carmine-client k vs)
      (is (= (r/lindex mock-client k idx) (r/lindex carmine-client k idx)))
      (dbs-equal mock-client carmine-client))))

;(defspec test-linsert)

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

(defspec test-lpushx
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k1 gen/string-alphanumeric
                   k2 gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))
                   v gen/int]
      (is (= (r/lpushx mock-client k1 v) (r/lpushx carmine-client k1 v)))
      (assert-rpush mock-client carmine-client k2 vs)
      (is (= (r/lpushx mock-client k2 v) (r/lpushx carmine-client k2 v)))
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

(defspec test-lrem
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))
                   cnt gen/int]
      (assert-rpush mock-client carmine-client k vs)
      (let [v (first (shuffle vs))]
        (is (= (r/lrem mock-client k cnt v) (r/lrem carmine-client k cnt v)))
        (dbs-equal mock-client carmine-client)))))

;;(defspec test-lset)
;;(defspec test-ltrim)

(defspec test-rpop
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   fake-key gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (assert-rpush mock-client carmine-client k vs)
      (is (= (r/rpop mock-client k) (r/rpop carmine-client k)))
      (is (= (r/rpop mock-client fake-key)
             (r/rpop carmine-client fake-key)))
      (dbs-equal mock-client carmine-client))))

(defspec test-rpush
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (is (= (r/rpush mock-client k vs) (r/rpush carmine-client k vs)))
      (dbs-equal mock-client carmine-client))))

; Sets
(defspec test-sadd
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k1 gen/string-alphanumeric
                   v gen/int
                   k2 gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (is (= (r/sadd mock-client k1 v) (r/sadd carmine-client k1 v)))
      (is (= (r/sadd mock-client k2 vs) (r/sadd carmine-client k2 vs)))
      (dbs-equal mock-client carmine-client))))

(defspec test-scard
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (assert-sadd mock-client carmine-client k vs)
      (is (= (r/scard mock-client k) (r/scard carmine-client k)))
      (dbs-equal mock-client carmine-client))))

(defspec test-sdiff
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k1 gen/string-alphanumeric
                   vs1 (gen/not-empty (gen/vector gen/int))

                   k2 gen/string-alphanumeric
                   vs2 (gen/not-empty (gen/vector gen/int))]
      (assert-sadd mock-client carmine-client k1 vs1)
      (assert-sadd mock-client carmine-client k2 vs2)
      (is (= (sort (r/sdiff mock-client k1)) (sort (r/sdiff carmine-client k1))))
      (is (= (sort (r/sdiff mock-client [k1 k2])) (sort (r/sdiff carmine-client [k1 k2]))))
      (dbs-equal mock-client carmine-client))))

(defspec test-sdiffstore
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [src gen/string-alphanumeric
                   dest gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (assert-sadd mock-client carmine-client src vs)
      (is (= (r/sdiffstore mock-client dest src) (r/sdiffstore carmine-client dest src)))
      (dbs-equal mock-client carmine-client))))

(defspec test-sinter
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k1 gen/string-alphanumeric
                   vs1 (gen/not-empty (gen/vector gen/int))

                   k2 gen/string-alphanumeric
                   vs2 (gen/not-empty (gen/vector gen/int))]
      (assert-sadd mock-client carmine-client k1 vs1)
      (assert-sadd mock-client carmine-client k2 vs2)
      (is (= (sort (r/sinter mock-client k1)) (sort (r/sinter carmine-client k1))))
      (is (= (sort (r/sinter mock-client [k1 k2])) (sort (r/sinter carmine-client [k1 k2]))))
      (dbs-equal mock-client carmine-client))))

(defspec test-sinterstore
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [src gen/string-alphanumeric
                   dest gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (assert-sadd mock-client carmine-client src vs)
      (is (= (r/sinterstore mock-client dest src) (r/sinterstore carmine-client dest src)))
      (dbs-equal mock-client carmine-client))))

(defspec test-sismember
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (assert-sadd mock-client carmine-client k vs)
      (let [v (first vs)]
        (is (= (r/sismember mock-client k v) (r/sismember carmine-client k v))))
      (is (= (r/sismember mock-client k "fake") (r/sismember carmine-client k "fake")))
      (dbs-equal mock-client carmine-client))))

(defspec test-smembers
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (assert-sadd mock-client carmine-client k vs)
      (is (= (sort (r/smembers mock-client k)) (sort (r/smembers carmine-client k))))
      (dbs-equal mock-client carmine-client))))

(defspec test-smove
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k1 gen/string-alphanumeric
                   k2 gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))

                   fake-1 gen/string-alphanumeric
                   fake-2 gen/string-alphanumeric
                   ]
      (assert-sadd mock-client carmine-client k1 vs)
      (let [v (first vs)]
        (is (= (r/smove mock-client k1 k2 v) (r/smove mock-client k1 k2 v)))
        (is (= (r/smove mock-client k1 k2 "fake") (r/smove mock-client k1 k2 "fake")))
        (is (= (r/smove mock-client fake-1 k2 v) (r/smove mock-client fake-1 k2 v)))
        (is (= (r/smove mock-client k1 fake-2 v) (r/smove mock-client k1 fake-2 v)))
      (dbs-equal mock-client carmine-client)))))

(defspec test-srem
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (assert-sadd mock-client carmine-client k vs)
      (let [v (first vs)]
        (is (= (r/srem mock-client k v) (r/srem carmine-client k v)))
        (is (= (r/srem mock-client k "fake") (r/srem carmine-client k "fake"))))
      (dbs-equal mock-client carmine-client))))

(defspec test-sunion
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [k1 gen/string-alphanumeric
                   vs1 (gen/not-empty (gen/vector gen/int))

                   k2 gen/string-alphanumeric
                   vs2 (gen/not-empty (gen/vector gen/int))]
      (assert-sadd mock-client carmine-client k1 vs1)
      (assert-sadd mock-client carmine-client k2 vs2)
      (is (= (sort (r/sunion mock-client k1)) (sort (r/sunion carmine-client k1))))
      (is (= (sort (r/sunion mock-client [k1 k2])) (sort (r/sunion carmine-client [k1 k2]))))
      (dbs-equal mock-client carmine-client))))

(defspec test-sunionstore
  10
  (let [mock-client (mock/->redis)]
    (prop/for-all [src gen/string-alphanumeric
                   dest gen/string-alphanumeric
                   vs (gen/not-empty (gen/vector gen/int))]
      (assert-sadd mock-client carmine-client src vs)
      (is (= (r/sunionstore mock-client dest src) (r/sunionstore carmine-client dest src)))
      (dbs-equal mock-client carmine-client))))
