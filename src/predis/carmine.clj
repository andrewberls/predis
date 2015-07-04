(ns predis.carmine
  (:require [taoensso.carmine :as car]
            (predis
              [core :as core]
              [util :as util])))

(def default-config
  {:pool {}
   :spec {:host "127.0.0.1" :port 6379}})

(deftype CarmineClient [config]
  core/IRedis
  ; Keys
  (core/del [this k-or-ks]
    (car/wcar config (apply car/del (util/vec-wrap k-or-ks))))

  (core/exists [this k]
    (car/wcar config (car/exists k)))

  (core/keys [this pat]
    (car/wcar config (car/keys pat)))

  (core/randomkey [this]
    (car/wcar config (car/randomkey)))

  (core/rename [this k new-k]
    (car/wcar config (car/rename k new-k)))

  (core/renamenx [this k new-k]
    (car/wcar config (car/renamenx k new-k)))

  (core/type [this k]
    (car/wcar config (car/type k)))

  (core/scan [this k]
    (core/scan this k {}))

  (core/scan [this cursor {:keys [match count]}]
    (let [match' (when match ["MATCH" match])
          count' (when count ["COUNT" count])
          scan-opts (->> (filter identity [match' count'])
                         (apply concat))]
      (if (empty? scan-opts)
        (car/wcar config (car/scan cursor))
        (car/wcar config (apply car/scan cursor scan-opts)))))

  ; Server
  (core/flushdb [this]
    (car/wcar config (car/flushdb)))

  (core/dbsize [this]
    (car/wcar config (car/dbsize)))

  ; Connection
  (core/ping [this]
    (car/wcar config (car/ping)))

  ; Strings
  (core/append [this k v]
    (car/wcar config (car/append k v)))

  (core/decr [this k]
    (car/wcar config (car/decr k)))

  (core/decrby [this k decrement]
    (car/wcar config (car/decrby k decrement)))

  (core/get [this k]
    (car/wcar config (car/get k)))

  (core/getrange [this k start end]
    (car/wcar config (car/getrange k start end)))

  (core/getset [this k v]
    (car/wcar config (car/getset k v)))

  (core/incr [this k]
    (car/wcar config (car/incr k)))

  (core/incrby [this k increment]
    (car/wcar config (car/incrby k increment)))

  (core/incrbyfloat [this k increment]
    (car/wcar config (car/incrbyfloat k increment)))

  (core/mget [this ks]
    (car/wcar config (apply car/mget ks)))

  (core/mset [this kvs]
    (car/wcar config (apply car/mset (apply concat kvs))))

  (core/msetnx [this kvs]
    (car/wcar config (apply car/msetnx (apply concat kvs))))

  (core/set [this k v]
    (car/wcar config (car/set k v)))

  (core/setnx [this k v]
    (car/wcar config (car/setnx k v)))

  (core/strlen [this k]
    (car/wcar config (car/strlen k)))

  ; Hashes
  (core/hdel [this k f-or-fs]
    (car/wcar config (apply car/hdel k (util/vec-wrap f-or-fs))))

  (core/hexists [this k f]
    (car/wcar config (car/hexists k f)))

  (core/hget [this k f]
    (car/wcar config (car/hget k f)))

  (core/hgetall [this k]
    (car/wcar config (car/hgetall k)))

  (core/hincrby [this k f increment]
    (car/wcar config (car/hincrby k f increment)))

  (core/hincrbyfloat [this k f increment]
    (car/wcar config (car/hincrbyfloat k f increment)))

  (core/hkeys [this k]
    (car/wcar config (car/hkeys k)))

  (core/hlen [this k]
    (car/wcar config (car/hlen k)))

  (core/hmget [this k f-or-fs]
    (car/wcar config (apply car/hmget k (util/vec-wrap f-or-fs))))

  (core/hmset [this k kvs]
    (car/wcar config (apply car/hmset k (flatten kvs))))

  (core/hset [this k f v]
    (car/wcar config (car/hset k f v)))

  (core/hsetnx [this k f v]
    (car/wcar config (car/hsetnx k f v)))

  (core/hvals [this k]
    (car/wcar config (car/hvals k)))

  ; Lists
  (core/lindex [this k idx]
    (car/wcar config (car/lindex k idx)))

  (core/linsert [this k pos pivot v]
    (car/wcar config (car/linsert k pos pivot v)))

  (core/llen [this k]
    (car/wcar config (car/llen k)))

  (core/lpop [this k]
    (car/wcar config (car/lpop k)))

  (core/lpush [this k v-or-vs]
    (car/wcar config (apply car/lpush k (util/vec-wrap v-or-vs))))

  (core/lpushx [this k v]
    (car/wcar config (car/lpushx k v)))

  (core/lrange [this k start stop]
    (car/wcar config (car/lrange k start stop)))

  (core/lrem [this k cnt v]
    (car/wcar config (car/lrem k cnt v)))

  (core/lset [this k idx v]
    (car/wcar config (car/lset k idx v)))

  (core/ltrim [this k start stop]
    (car/wcar config (car/ltrim k start stop)))

  (core/rpop [this k]
    (car/wcar config (car/rpop k)))

  (core/rpush [this k v-or-vs]
    (car/wcar config (apply car/rpush k (util/vec-wrap v-or-vs))))

  (core/rpushx [this k v]
    (car/wcar config (car/rpushx k v)))

  ; Sets
  (core/sadd [this k m-or-ms]
    (car/wcar config (apply car/sadd k (util/vec-wrap m-or-ms))))

  (core/scard [this k]
    (car/wcar config (car/scard k)))

  (core/sdiff [this k-or-ks]
    (car/wcar config (apply car/sdiff (util/vec-wrap k-or-ks))))

  (core/sdiffstore [this dest k-or-ks]
    (car/wcar config (apply car/sdiffstore dest (util/vec-wrap k-or-ks))))

  (core/sinter [this k-or-ks]
    (car/wcar config (apply car/sinter (util/vec-wrap k-or-ks))))

  (core/sinterstore [this dest k-or-ks]
    (car/wcar config (apply car/sinterstore dest (util/vec-wrap k-or-ks))))

  (core/sismember [this k m]
    (car/wcar config (car/sismember k m)))

  (core/smembers [this k]
    (car/wcar config (car/smembers k)))

  (core/smove [this src dest m]
    (car/wcar config (car/smove src dest m)))

  (core/spop [this k]
    (car/wcar config (car/spop k)))

  ; TODO
  (core/srandmember [this k]
    (first (core/srandmember this k 1)))
  ;(srandmember [this k cnt])

  (core/srem [this k m-or-ms]
    (car/wcar config (apply car/srem k (util/vec-wrap m-or-ms))))

  (core/sunion [this k-or-ks]
    (car/wcar config (apply car/sunion (util/vec-wrap k-or-ks))))

  (sunionstore [this dest k-or-ks]
    (car/wcar config (apply car/sunionstore dest (util/vec-wrap k-or-ks))))

  (zadd [this k score m]
    (core/zadd this k [[score m]]))

  (zadd [this k kvs]
    (car/wcar config (apply car/zadd k (apply concat kvs))))

  (zcard [this k]
    (car/wcar config (car/zcard k)))

  (zcount [this k min-score max-score]
    (car/wcar config (car/zcount k min-score max-score)))

  (zincrby [this k increment m]
    (car/wcar config (car/zincrby k increment m)))

  ;;(zinterstore [this dest numkeys ks weights])
  ;;(zlexcount [this k min-val max-val])

  (zrange [this k start stop]
    (core/zrange this k start stop {}))

  (zrange [this k start stop {:keys [withscores]}]
    (if withscores
      (car/wcar config (car/zrange k start stop "WITHSCORES"))
      (car/wcar config (car/zrange k start stop))))

  ;;(zrangebylex [this k min-val max-val opts?])

  (zrangebyscore [this k min-score max-score]
    (core/zrangebyscore this k min-score max-score {}))

  ; TODO: support offset, count
  (zrangebyscore [this k min-score max-score {:keys [withscores offset count]}]
    (let [
          ;withscores' (when withscores ["MATCH" match])
          ]
      (if withscores
        (car/wcar config (car/zrangebyscore k min-score max-score "WITHSCORES"))
        (car/wcar config (car/zrangebyscore k min-score max-score)))))

  (zrank [this k m]
    (car/wcar config (car/zrank k m)))

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

(defn ->redis
  ([]
   (->redis default-config))
  ([config]
   (->CarmineClient config)))
