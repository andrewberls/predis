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

(defn normalized-end-idx
  "Given an end idx which may be negative (indicating offset from the end)
   or exceed the size of xs, return a normalized positive 0-based idx

   Semantics for end differ from start - e.g. LRANGE specifies that
   end > last-idx = treat like last idx"
  [xs end]
  (let [len (count xs)
        last-idx (dec len)]
    (cond
      (> end last-idx) last-idx
      (< end 0) (+ len end)
      :else end)))

(defn indices-for
  "Normalize user-specified indices into a range of positive indices
   It is up to the user to check for additional error cases as necessary"
  [xs start end]
  (let [last-idx (dec (count xs))
        start' (normalized-start-idx xs start)
        end' (normalized-end-idx xs end)]
    (range start' (inc end'))))
