(ns predis.util.zset)

(defn zset-compare [[member-a score-a] [member-b score-b]]
  (let [score-comp (compare score-a score-b)]
    (if (zero? score-comp)
      (compare member-a member-b)
      score-comp)))

(defn sort-zset [zset]
  (sort zset-compare zset))

(defn parse-zmin [score]
  (if (= score "-inf") Float/NEGATIVE_INFINITY (long score)))

(defn parse-zmax [score]
  (if (= score "+inf") Float/POSITIVE_INFINITY (long score)))

; TODO: exclusive range "(5"
(defn zmember-in-range? [min-score max-score [m score :as zmember]]
  (let [min-score' (parse-zmin min-score)
        max-score' (parse-zmax max-score)]
    (and
      (>= score min-score')
      (<= score max-score'))))

(defn zrangebyscore
  "Filter members of zset within min-score and max-score and return
   a seq of [member score] tuples sorted by ascending score"
  [min-score max-score zset]
  (->> (filter (partial zmember-in-range? min-score max-score) zset)
       (sort-zset)))

(defn zset-response
  "Given a seq of pre-sorted [member score] tuples, return an appropriate
   response based on the boolean withscores option"
  [tups withscores]
  (if withscores
    (apply concat tups)
    (map first tups)))
