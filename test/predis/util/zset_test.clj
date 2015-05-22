(ns predis.util.zset-test
  (:require [clojure.test :refer :all]
            [predis.util.zset :as util.zset]))

(deftest test-sort-zset
  (let [z1 {"foo" 1 "far" 2 "ba" 3 "bz" 3 "quux" 4}
        z2 {}]
    (is (= [["foo" 1] ["far" 2] ["ba" 3] ["bz" 3] ["quux" 4]]
           (util.zset/sort-zset z1)))
    (is (= [] (util.zset/sort-zset z2)))))

(deftest test-zrangebyscore
  (let [z1 {"foo" 1 "far" 2 "ba" 3 "bz" 3 "quux" 4}
        z2 {}]
    (is (= [["foo" 1] ["far" 2] ["ba" 3] ["bz" 3] ["quux" 4]]
           (util.zset/zrangebyscore "-inf" "+inf" z1)))
    (is (= [["far" 2] ["ba" 3] ["bz" 3]] (util.zset/zrangebyscore 2 3 z1)))
    (is (= [["ba" 3] ["bz" 3] ["quux" 4]] (util.zset/zrangebyscore 3 100 z1)))
    (is (= [] (util.zset/zrangebyscore 5 10 z2)))))
