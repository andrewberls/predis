(ns predis.util.range
  "Utilities for interpreting/normalizing indices for range commands,
   e.g. LRANGE, ZRANGE")

(defn normalized-start-idx
  "Given an start idx which may be negative (indicating offset from the end)
   or exceed the size of xs, return a normalized positive 0-based idx

   Semantics for start differ from end - e.g. LRANGE specifies that
   start > last-idx = empty list"
  [xs start]
  (let [len (count xs)
        last-idx (dec len)]
    (cond
      (< start (- len)) 0
      (< start 0) (+ len start)
      :else start)))

(defn normalized-stop-idx
  "Given a stop idx which may be negative (indicating offset from the end)
   or exceed the size of xs, return a normalized positive 0-based idx

   Semantics for stop differ from start - e.g. LRANGE specifies that
   stop > last-idx = treat like last idx"
  [xs stop]
  (let [len (count xs)
        last-idx (dec len)]
    (cond
      (> stop last-idx) last-idx
      (< stop 0) (+ len stop)
      :else stop)))

(defn indices-for
  "Normalize user-specified indices into a range of positive indices
   It is up to the user to check for additional error cases as necessary"
  [xs start stop]
  (let [last-idx (dec (count xs))
        start' (normalized-start-idx xs start)
        stop' (normalized-stop-idx xs stop)]
    (range start' (inc stop'))))
