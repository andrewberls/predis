(ns predis.mock
  (:require [clojure.set :as clj.set]
            [clojure.string :as string]
            (predis
              [core :as core]
              [util :as util])))

(def err-wrongtype
  "WRONGTYPE Operation against a key holding the wrong kind of value")

(def err-badint
 "ERR value is not an integer or out of range")

(def err-srcdest
 "ERR source and destination objects are the same")

(defn err-arity [cmd]
  (format "ERR wrong number of arguments for %s" cmd))

;;

(defn- set-at [redis k]
  (if-let [vs (core/get redis k)]
    (set vs)))

(defn normalized-start-idx
  "Given an start idx which may be negative (indicating offset from the end)
   or exceed the size of xs, return a normalized positive 0-based idx
   See http://redis.io/commands/lrange"
  [xs start]
  (let [last-idx (dec (count xs))
        start' (cond
                 (> start last-idx) last-idx
                 (< start 0) (+ (count xs) start)
                 :else start)]
    ; TODO
    (if (and (< start' 0) (> (Math/abs start') last-idx))
      0
      start')))

(defn normalized-end-idx
  "Given an end idx which may be negative (indicating offset from the end)
   or exceed the size of xs, return a normalized positive 0-based idx
   See http://redis.io/commands/lrange"
  [xs end]
  (let [last-idx (dec (count xs))]
    (cond
      (> end last-idx) last-idx
      (< end 0) (+ (count xs) end)
      :else end)))

;;

(deftype MockClient [store]
  core/IRedis
  ;; Keys
  (core/del [this k-or-ks]
    (let [ks' (util/vec-wrap k-or-ks)
          [store' nremoved] (util/counting-dissoc @store ks')]
      (reset! store store')
      nremoved))

  (core/exists [this k]
    (if (core/get this k) 1 0))

  (core/keys [this pat]
    (let [re (re-pattern (string/escape pat {\* ".*" \? ".?"}))
          key-matches
          (fn [k]
            (let [match (re-find re k)]
              ; Bit of hackery to support empty keys (legal!)
              (not (and (empty? match) (not= match k)))))]
      (filter key-matches (keys @store))))

  (core/randomkey [this]
    (first (shuffle (keys @store))))

  (core/rename [this k new-k]
    (assert (not= k new-k) err-srcdest)
    (core/set this new-k (core/get this k))
    (core/del this k)
    "OK")

  (core/renamenx [this k new-k]
    (if-not (core/get this new-k)
      (do
        (core/rename this k new-k)
        1)
      0))

  (core/type [this k]
    (let [x (core/get this k)]
      (cond
        (string? x) "string"
        (set? x) "set"
        (sequential? x) "list"
        (associative? x) "hash"
        :else "none")))

  (core/scan [this cursor]
    (core/scan this cursor {}))

  (core/scan [this cursor {:keys [match count] :as opts}]
    (let [pat (or match "*")
          ks (core/keys this pat)
          ks' (if count
                (take count ks)
                ks)]
      [cursor ks']))

  ;; Server
  (core/flushdb [this]
    (reset! store {})
    "OK")

  ;; Connection
  (core/ping [this] "PONG")

  ;; Strings
  (core/append [this k v]
    (let [do-append (fn [old-str] (str old-str v))]
      (swap! store update-in [k] do-append)
      (core/strlen this k)))

  (core/decr [this k]
    (core/decrby this k 1))

  (core/decrby [this k decrement]
    (let [do-decrby (fn [old]
                      (->> (- (if old (do (assert (string? old)) (Integer/parseInt old)) 0) decrement)
                           str))]
      (swap! store update-in [k] do-decrby)
      (Integer/parseInt (core/get this k))))

  (core/get [this k]
    (get @store (str k)))

  (core/incr [this k]
    (core/incrby this k 1))

  (core/incrby [this k increment]
    (let [do-incrby (fn [old]
                      (->> (+ (if old (Integer/parseInt old) 0) increment)
                           str))]
    (swap! store update-in [k] do-incrby)
    (Integer/parseInt (core/get this k))))

  (core/incrbyfloat [this k increment]
    (let [do-incrby (fn [old]
                      (->> (+ (if old (Double/parseDouble old) 0) increment)
                           str))]
    (swap! store update-in [k] do-incrby)
    (core/get this k)))

  (core/mget [this ks]
    (util/values-at @store ks))

  (core/mset [this kvs]
    (assert (even? (count kvs)) (err-arity "MSET"))
    (doseq [[k v] kvs]
      (core/set this k v))
    "OK")

  (core/set [this k v]
    (swap! store assoc (str k) (str v))
    "OK")

  (core/setnx [this k v]
    (if-not (core/get this k)
      (do
        (core/set this k v)
        1)
      0))

  (core/strlen [this k]
    (let [s (or (core/get this k) "")]
      (assert (string? s) err-wrongtype)
      (count s)))

  ;; Hashes
  (core/hdel [this k f-or-fs]
    (if-let [m (core/get this k)]
      (let [[m' nremoved] (->> (util/vec-wrap f-or-fs)
                               (util/counting-dissoc m))]
        (swap! store assoc k m')
        nremoved)
      0))

  (core/hexists [this k f]
    (if (core/hget this k f) 1 0))

  (core/hget [this k f]
    (let [m (or (core/get this k) {})]
      (get m f)))

  (core/hgetall [this k]
    (let [vs (seq (or (core/get this k) []))]
      (or vs [])))

  ; TODO: str
  (core/hincrby [this k f increment]
    (let [do-hincrby (fn [m]
                       (let [old (get m f 0)]
                         (assoc m f (+ old increment))))]
      (swap! store update-in [k] do-hincrby)
      (core/hget this k f)))

  (core/hincrbyfloat [this k f increment]
    (core/hincrby this k f increment))

  (core/hlen [this k]
    (let [m (or (core/get this k) {})]
      (count (keys m))))

  (core/hmget [this k f-or-fs]
    (let [m (or (core/get this k) {})
          fs' (util/vec-wrap f-or-fs)]
      (util/values-at m fs')))

  ; TODO: tuples
  (core/hmset [this k kvs]
    (let [do-merge (fn [m]
                     (->> (concat (seq (or m {})) kvs)
                          (into {})))]
      (swap! store update-in [k] do-merge)
      "OK"))

  (core/hvals [this k]
    (let [m (or (core/get this k) {})]
      (or (vals (sort m)) [])))

  ;; Lists
  (core/llen [this k]
    (count (core/get this k)))

  (core/lpop [this k]
    (let [vs (core/get this k)]
      (when (seq vs)
        (let [v (first vs)
              vs' (rest vs)]
          (if (seq vs')
            (swap! store assoc k vs')
            (swap! store dissoc k))
          v))))

  (core/lpush [this k v-or-vs]
    (let [vs' (util/vec-wrap v-or-vs)
          do-push (fn [old-vs new-v] (cons new-v (or old-vs [])))]
      (doseq [v vs']
        (swap! store update-in [k] do-push (str v)))
      (core/llen this k)))

  (core/lrange [this k start stop]
    (let [vs (vec (core/get this k))
          last-idx (dec (count vs))
          start' (normalized-start-idx vs start)
          stop' (normalized-end-idx vs stop)
          indices (range start' (inc stop'))]
      (if (seq vs)
        (if (> start last-idx)
          []
          (map (partial get vs) indices))
        [])))

  (core/lrem [this k cnt v]
    (assert (number? cnt) err-badint)
    (let [vs (core/get this k)]
      (if (seq vs)
        (let [v' (str v)
              [vs' nremoved] (cond
                               (> cnt 0) (util/remove-first-n vs cnt v')
                               (< cnt 0) (util/remove-last-n vs (Math/abs cnt) v')
                               (= cnt 0) (util/remove-all vs v'))]
          (swap! store assoc k vs')
          nremoved)
        0)))

  (core/rpop [this k]
    (let [vs (core/get this k)]
      (when (seq vs)
        (let [v (last vs)
              vs' (butlast vs)]
          (if (seq vs')
            (swap! store assoc k vs')
            (swap! store dissoc k))
          v))))

  (core/rpush [this k v-or-vs]
    (let [vs' (util/vec-wrap v-or-vs)
          do-push (fn [old-vs] (concat (or old-vs []) (map str vs')))]
      (swap! store update-in [k] do-push)
      (core/llen this k)))

  ; Sets
  (core/sadd [this k m-or-ms]
    (let [s (or (core/get this k) #{})
          [s' nadded] (->> (util/vec-wrap m-or-ms)
                           (util/counting-union s))]
      (swap! store assoc k s')
      nadded))

  (core/scard [this k]
    (count (core/get this k)))

  (core/sdiff [this k-or-ks]
    (let [ks' (util/vec-wrap k-or-ks)
          diff (->> (apply clj.set/difference
                      (set-at this (first ks'))
                      (map (partial set-at this) (rest ks')))
                    seq)]
      (or diff [])))

  (core/sdiffstore [this dest k-or-ks]
    (let [diff (set (core/sdiff this k-or-ks))]
      (swap! store assoc dest diff)
      (core/scard this dest)))

  (core/sinter [this k-or-ks]
    (let [ks' (util/vec-wrap k-or-ks)
          inter (->> (apply clj.set/intersection
                       (set-at this (first ks'))
                       (map (partial set-at this) (rest ks')))
                     seq)]
      (or inter [])))

  (core/sinterstore [this dest k-or-ks]
    (let [inter (set (core/sinter this k-or-ks))]
      (swap! store assoc dest inter)
      (core/scard this dest)))

  (core/sismember [this k m]
    (contains? (set-at this k) (str m)))

  (core/smembers [this k]
    (seq (set-at this k)))

  (core/smove [this src dest m]
    (if-let [s (set-at this src)]
      (if (contains? s m)
        (do
          (core/srem this src m)
          (core/sadd this dest m)
          1)
      0)))

  (core/spop [this k]
    (if-let [s (get this k)]
      (let [m (first (shuffle s))]
        (core/srem this k m)
        m)))

  (core/srandmember [this k]
    (first (core/srandmember this k 1)))

  (core/srandmember [this k cnt]
    (when-let [s (get this k)]
      (take cnt (repeatedly #(first (shuffle s))))))

  (core/srem [this k m-or-ms]
    (let [ms' (util/vec-wrap m-or-ms)]
      (if-let [s (set-at this k)]
        (let [[s' nremoved] (util/counting-disj s ms')]
          (swap! store assoc k s')
          nremoved)
        0)))

  (sunion [this k-or-ks]
    (let [ks' (util/vec-wrap k-or-ks)]
      (apply clj.set/union
        (set-at this (first ks'))
        (map (partial set-at this) (rest ks')))))

  (core/sunionstore [this dest k-or-ks]
    (let [union (core/sunion this k-or-ks)]
      (swap! store assoc dest union)
      (core/scard this dest))))

(defn ->redis
  ([]
   (->redis {}))
  ([init-state]
   (->MockClient (atom init-state))))
