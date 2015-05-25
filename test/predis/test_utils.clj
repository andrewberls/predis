(ns predis.test-utils
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            (clojure.test.check
              [clojure-test :refer [defspec]]
              [generators :as gen]
              [properties :as prop])
            [taoensso.carmine :as carmine]
            (predis
              [core :as r]
              [carmine :as predis.carmine])))

(def redis-config
  {:pool {}
   :spec {:host "127.0.0.1" :port 6379}})

(defn carmine-client []
  (predis.carmine/->redis redis-config))

(defn flush-redis [f]
  (carmine/wcar redis-config (carmine/flushdb))
  (f))

(def gen-ne-str (gen/not-empty gen/string-alphanumeric))

(def gen-hash-kv
  (gen/tuple
    gen-ne-str   ; Field
    gen-ne-str)) ; Value

(def gen-kvs-vec
  (gen/not-empty
    (gen/vector (gen/tuple
                   gen/int        ; Score
                   gen-ne-str)))) ; Member

(defn get-any [client k]
  (condp = (r/type client k)
    "string" (r/get client k)
    "list" (r/lrange client k 0 -1)
    "hash" (sort (r/hgetall client k))
    "set" (sort (r/smembers client k))
    "zset" (r/zrangebyscore client k "-inf" "+inf" {:withscores true})
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

(defn assert-zadd [c1 c2 k kvs]
  (is (= (r/zadd c1 k kvs) (r/zadd c2 k kvs))))

(defn dbs-equal [c1 c2]
  (is (= (db c1) (db c2))))
