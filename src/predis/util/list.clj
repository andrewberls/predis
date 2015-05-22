(ns predis.util.list)

(defn normalized-start-idx
  "Given an start idx which may be negative (indicating offset from the end)
   or exceed the size of xs, return a normalized positive 0-based idx
   See http://redis.io/commands/lrange"
  [xs start]
  (let [len (count xs)
        last-idx (dec len)]
    (cond
      (< start (- len)) 0
      (< start 0) (+ len start)
      :else start)))

(defn normalized-end-idx
  "Given an end idx which may be negative (indicating offset from the end)
   or exceed the size of xs, return a normalized positive 0-based idx
   See http://redis.io/commands/lrange"
  [xs end]
  (let [len (count xs)
        last-idx (dec len)]
    (cond
      (> end last-idx) last-idx
        (< end 0) (+ len end)
        :else end)))
