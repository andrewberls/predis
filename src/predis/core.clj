(ns predis.core
  (:refer-clojure :exclude [get set keys type]))

(defprotocol IRedis
  ; Keys
  (del [this k-or-ks])
  (exists [this k])
  (keys [this pat])
  (randomkey [this])
  (rename [this k new-k])
  (renamenx [this k new-k])
  (type [this k])
  (scan [this cursor] [this cursor opts]
    "opts - Optional map of
      :match - String key pattern, e.g. \"*\"
      :count - Integer count hint, e.g. 3
     Ex:
       (scan \"0\")
       (scan \"0\" {:match \"my-prefix*\"})
       (scan \"0\" {:match \"my-prefix*\" :count 10})")

  ; Server
  (flushdb [this])
  (dbsize [this])

  ; Connection
  (ping [this])

  ; Strings
  (append [this k v])
  (decr [this k])
  (decrby [this k decrement])
  (get [this k])
  (getrange [this k start end])
  (incr [this k])
  (incrby [this k increment])
  (incrbyfloat [this k increment])
  (mget [this ks])
  (mset [this kvs]
    "Ex: (mset [[\"foo\" \"1\"] [\"bar\" \"2\"]])")
  (set [this k v])
  (setnx [this k v])
  (strlen [this k])

  ; Hashes
  (hdel [this k f-or-fs]
    "Ex:
       (hdel \"my-hash\" \"my-field\")
       (hdel \"my-hash\" [\"field-a\" \"field-b\"])")
  (hexists [this k f])
  (hget [this k f])
  (hgetall [this k])
  (hincrby [this k f increment])
  (hincrbyfloat [this k f increment])
  (hkeys [this k])
  (hlen [this k])
  (hmget [this k f-or-fs]
    "Ex:
       (hmget \"my-hash\" \"my-field\")
       (hmget \"my-hash\" [\"field-a\" \"field-b\"])")
  (hmset [this k kvs]
    "Ex:
       (hmset \"my-hash\" [[\"field-a\" \"val-a\"] [\"field-b\" \"val-b\"]])")
  (hset [this k f v])
  (hsetnx [this k f v])
  (hvals [this k])

  ; Lists
  (lindex [this k idx])
  ;(linsert [k pos pivot v])
  (llen [this k])
  (lpop [this k])
  (lpush [this k v-or-vs]
    "Ex:
       (lpush \"my-list\" 2)
       (lpush \"my-list\" [1 2 3 4])")
  (lpushx [this k v])
  (lrange [this k start stop])
  (lrem [this k cnt v])
  (lset [this k idx v])
  ;(ltrim [this k start stop])
  (rpop [this k])
  ;(rpoplpush [this src dest])
  (rpush [this k v-or-vs]
    "Ex:
       (rpush \"my-list\" 2)
       (rpush \"my-list\" [1 2 3 4])")
  (rpushx [this k v])

  ; Sets
  (sadd [this k m-or-ms]
    "Ex:
       (sadd \"my-set\" 2)
       (sadd \"my-set\" [1 2 3 4])")
  (scard [this k])
  (sdiff [this k-or-ks])
  (sdiffstore [this dest k-or-ks])
  (sinter [this k-or-ks])
  (sinterstore [this dest k-or-ks])
  (sismember [this k m])
  (smembers [this k])
  (smove [this src dest m])
  (spop [this k])
  (srandmember [this k] [this k cnt])
  (srem [this k m-or-ms])
  (sunion [this k-or-ks])
  (sunionstore [this dest k-or-ks])

  ; Sorted Sets
  )
