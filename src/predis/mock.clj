(ns predis.mock
  (:require [clojure.set :as clj.set]
            [clojure.string :as string]
            (predis
              [core :as core]
              [util :as util])
            (predis.util
              [range :as util.range]
              [zset :as util.zset])))

(def err-wrongtype
  "WRONGTYPE Operation against a key holding the wrong kind of value")

(def err-badint
 "ERR value is not an integer or out of range")

(def err-srcdest
 "ERR source and destination objects are the same")

(defn err-arity [cmd]
  (format "ERR wrong number of arguments for %s" cmd))

;;

; Need to distinguish types internally, both represented
; with Clojure maps
(def empty-zset (with-meta {} {:predis/type "zset"}))
(def empty-hash (with-meta {} {:predis/type "hash"}))

(defn- set-at [redis k]
  (set (core/get redis k)))

(defn- hash-at [redis k]
  (or (core/get redis k) empty-hash))

(defn- zset-at [redis k]
  (or (core/get redis k) empty-zset))

(defn- replace-seq
  "If s is a non-empty seq, replace it at key k in store
   Otherwise remove it from the store"
  [store k xs]
  (if (seq xs)
    (swap! store assoc k xs)
    (swap! store dissoc k)))

;;

(deftype MockClient [store]
  core/IRedis
  ;; Keys
  (core/del [this k-or-ks]
    (let [[store' nremoved] (->> (util/vec-wrap k-or-ks)
                                 (util/counting-dissoc @store))]
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
        (map? x) (let [t (:predis/type (meta x))]
                   (assert t "Invalid metadata!")
                   t)
        (sequential? x) "list"
        :else "none")))

  (core/scan [this cursor]
    (core/scan this cursor {}))

  (core/scan [this cursor {:keys [match count] :as opts}]
    ; Note - `count` is a hint for the implementation, and
    ; is ignored here
    (let [pat (or match "*")
          ks (core/keys this pat)]
      ["0" ks]))

  ;; Server
  (core/flushdb [this]
    (reset! store {})
    "OK")

  (core/dbsize [this]
    (count (keys @store)))

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
                      (->> (- (if old (Integer/parseInt old) 0) decrement)
                           str))]
      (swap! store update-in [k] do-decrby)
      (Integer/parseInt (core/get this k))))

  (core/get [this k]
    (get @store (str k)))

  (core/getset [this k v]
    (when-let [old (core/get this k)]
      (core/set this k v)
      old))

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
        (replace-seq store k m')
        nremoved)
      0))

  (core/hexists [this k f]
    (if (core/hget this k f) 1 0))

  (core/hget [this k f]
    (let [m (hash-at this k)]
      (get m (str f))))

  (core/hgetall [this k]
    (let [vs (seq (or (core/get this k) []))]
      (apply concat (or vs []))))

  (core/hincrby [this k f increment]
    (let [do-hincrby (fn [old]
                       (let [m (or old empty-hash)
                             old-v (Integer/parseInt (get m (str f) "0"))]
                         (assoc m f (str (+ old-v increment)))))]
      (swap! store update-in [k] do-hincrby)
      (Integer/parseInt (core/hget this k f))))

  (core/hincrbyfloat [this k f increment]
    (let [do-hincrby (fn [m]
                       (let [old (Double/parseDouble (get m (str f) "0"))]
                         (assoc m f (str (+ old increment)))))]
      (swap! store update-in [k] do-hincrby)
      (Double/parseDouble (core/hget this k f))))

  (core/hkeys [this k]
    (or (keys (core/get this k))
        []))

  (core/hlen [this k]
    (let [m (hash-at this k)]
      (count (keys m))))

  (core/hmget [this k f-or-fs]
    (let [m (hash-at this k)
          fs' (util/vec-wrap f-or-fs)]
      (util/values-at m fs')))

  (core/hmset [this k kvs]
    (let [kvs' (map util/stringify-tuple kvs)
          do-merge (fn [m]
                     (->> (concat (seq (or m {})) kvs')
                          (into empty-hash)))]
      (swap! store update-in [k] do-merge)
      "OK"))

  (core/hset [this k f v]
    (let [m (hash-at this k)
          do-hset (fn [old] (assoc (or old empty-hash) (str f) (str v)))]
      (if (contains? m f)
        (do
          (swap! store update-in [k] do-hset)
          0)
        (do
          (swap! store update-in [k] do-hset)
          1))))

  (core/hsetnx [this k f v]
    (let [m (hash-at this k)]
      (if (contains? m f)
        0
        (do
          (core/hset this k f v)
          1))))

  (core/hvals [this k]
    (let [m (hash-at this k)]
      (or (vals (sort m)) [])))

  ;; Lists
  (core/lindex [this k idx]
    (let [vs (vec (core/get this k))
          last-idx (dec (count vs))
          idx' (util.range/normalized-end-idx vs idx)]
      ; Differ from lrange here
      (when (<= idx last-idx)
        (get vs idx'))))

  (core/llen [this k]
    (count (core/get this k)))

  (core/lpop [this k]
    (let [vs (core/get this k)]
      (when (seq vs)
        (let [v (first vs)
              vs' (rest vs)]
          (replace-seq store k vs')
          v))))

  (core/lpush [this k v-or-vs]
    (let [vs' (util/vec-wrap v-or-vs)
          do-push (fn [old-vs new-v] (cons new-v (or old-vs [])))]
      (doseq [v vs']
        (swap! store update-in [k] do-push (str v)))
      (core/llen this k)))

  (core/lpushx [this k v]
    (let [vs (core/get this k)]
      (if (seq vs)
        (core/lpush this k v)
        (core/llen this k))))

  (core/lrange [this k start end]
    (let [vs (vec (core/get this k))]
      (if (or (empty? vs) (> start (dec (count vs))))
        []
        (->> (util.range/indices-for vs start end)
             (map (partial get vs))))))

  (core/lrem [this k cnt v]
    (assert (number? cnt) err-badint)
    (let [vs (core/get this k)]
      (if (seq vs)
        (let [v' (str v)
              [vs' nremoved] (cond
                               (> cnt 0) (util/remove-first-n vs cnt v')
                               (< cnt 0) (util/remove-last-n vs (Math/abs cnt) v')
                               (= cnt 0) (util/remove-all vs v'))]
          (replace-seq store k vs')
          nremoved)
        0)))

  (core/rpop [this k]
    (let [vs (core/get this k)]
      (when (seq vs)
        (let [v (last vs)
              vs' (butlast vs)]
          (replace-seq store k vs')
          v))))

  (core/rpush [this k v-or-vs]
    (let [vs' (util/vec-wrap v-or-vs)
          do-push (fn [old-vs] (concat (or old-vs []) (map str vs')))]
      (swap! store update-in [k] do-push)
      (core/llen this k)))

  (core/rpushx [this k v]
    (let [vs (core/get this k)]
      (if (seq vs)
        (core/rpush this k v)
        (core/llen this k))))

  ; Sets
  (core/sadd [this k m-or-ms]
    (let [s (or (core/get this k) #{})
          [s' nadded] (->> (util/vec-wrap m-or-ms)
                           (map str)
                           (util/counting-union s))]
      (swap! store assoc k (set (map str s')))
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
    (if (contains? (set-at this k) (str m)) 1 0))

  (core/smembers [this k]
    (seq (set-at this k)))

  (core/smove [this src dest m]
    (if (contains? (set-at this src) m)
      (do
        (core/srem this src m)
        (core/sadd this dest m)
        1)
    0))

  (core/spop [this k]
    (if-let [s (core/get this k)]
      (let [m (first (shuffle s))]
        (core/srem this k m)
        m)))

  (core/srandmember [this k]
    (first (core/srandmember this k 1)))

  (core/srandmember [this k cnt]
    (when-let [s (get this k)]
      (take cnt (shuffle s))))

  (core/srem [this k m-or-ms]
    (let [ms' (map str (util/vec-wrap m-or-ms))
          s (set-at this k)]
      (if (seq s)
        (let [[s' nremoved] (util/counting-disj s ms')]
          (replace-seq store k s')
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
      (core/scard this dest)))

  ; Sorted Sets
  (core/zadd [this k score m]
    (let [m' (str m)
          zset (zset-at this k)
          do-zadd (fn [_] (assoc zset m' (long score)))
          ret (if (contains? zset m') 0 1)]
      (swap! store update-in [k] do-zadd)
      ret))

  (core/zadd [this k kvs]
    (reduce (fn [acc [score m]] (+ acc (core/zadd this k score m)))
            0
            kvs))

  (zcard [this k]
    (let [zset (zset-at this k)]
      (count zset)))

  (zcount [this k min-score max-score]
    (->> (zset-at this k)
         (util.zset/zrangebyscore min-score max-score)
         count))

  (zincrby [this k increment m]
    (let [do-zincrby
          (fn [old]
            (let [zset (or old empty-zset)
                  new-score (+ (get zset m 0.0) increment)]
              (assoc zset m new-score)))]
      (swap! store update-in [k] do-zincrby)
      (str (get (core/get this k) m))))

  ;;(zinterstore [this dest numkeys ks weights])
  ;;(zlexcount [this k min-val max-val])

  (core/zrange [this k start end]
    (core/zrange this k start end {}))

  (core/zrange [this k start end {:keys [withscores]}]
    (let [zset (vec (util.zset/sort-zset (zset-at this k)))]
      (if (or (empty? zset) (> start (dec (count zset))))
        []
        (let [tups (->> (util.range/indices-for zset start end)
                        (map (partial get zset))
                        (map util/stringify-tuple))]
          (util.zset/zset-response tups withscores)))))

  ;;(zrangebylex [this k min-val max-val opts?])

  (core/zrangebyscore [this k min-score max-score]
    (core/zrangebyscore this k min-score max-score {}))

  ; TODO: offset, count
  (core/zrangebyscore [this k min-score max-score {:keys [withscores offset count]}]
    (let [tups (->> (zset-at this k)
                    (util.zset/zrangebyscore min-score max-score)
                    (map util/stringify-tuple))]
      (util.zset/zset-response tups withscores)))

  (zrank [this k m]
    (let [zset (zset-at this k)]
      (when (seq zset)
        (let [vs (->> (util.zset/sort-zset zset)
                      (mapv first))]
          (.indexOf vs m)))))

  ;(zrem [this k m-or-ms])
  ;;(zremrangebylex [this k min-val max-val])
  ;(zremrangebyscore [this k min-score max-score])
  ;(zrevrange [this k start stop] [this k start stop opts])
  ;(zrevrangebyscore [this k max-score min-score opts])
  ;(zrevrank [this k m])
  ;(zscore [this k m])
  ;(zunionstore [dest numkeys ks weights])
  ;;(zscan [this k cursor] [this k cursor opts])
  )

;;

(defn ->redis
  ([]
   (->redis {}))
  ([init-state]
   (->MockClient (atom init-state))))
