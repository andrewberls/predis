(ns predis.util.zset)

(defn zset-compare [[val-a score-a] [val-b score-b]]
  (let [score-comp (compare score-a score-b)]
    (if (zero? score-comp)
      (compare val-a val-b)
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

(defn zrangebyscore [min-score max-score zset]
  (->> (filter (partial zmember-in-range? min-score max-score) zset)
       (sort-zset)))
