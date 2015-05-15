(ns predis.util-test
  (:require [clojure.test :refer :all]
            [predis.util :as util]))

(deftest test-values-at
  (is (= [1 nil] (util/values-at {:foo 1} [:foo :bar]))))

(deftest test-counting-disj
  (let [s1 {:foo 1 :bar 2 :quux 3}]
    (is (= [{:quux 3} 2] (util/counting-dissoc s1 [:foo :bar])))
    (is (= [{:bar 2 :quux 3} 1] (util/counting-dissoc s1 [:foo :fake-1 :fake-2])))
    (is (= [{:foo 1 :bar 2 :quux 3} 0] (util/counting-dissoc s1 [:fake-1 :fake-2])))))

(deftest test-counting-disj
  (let [s1 #{1 2 3 4 5}]
    (is (= [#{4 5} 3] (util/counting-disj s1 [1 2 3])))
    (is (= [#{1 2 3} 2] (util/counting-disj s1 [4 5 98 99])))
    (is (= [#{1 2 3 4 5} 0] (util/counting-disj s1 [98 99])))))

(deftest test-counting-union
  (let [s1 #{1 2 3}]
    (is (= [#{1 2 3 4} 1] (util/counting-union s1 [4])))
    (is (= [#{1 2 3 4 5} 2] (util/counting-union s1 [1 2 4 5])))
    (is (= [#{1 2 3} 0] (util/counting-union s1 [2 3])))))

(deftest test-remove-first-n
  (let [l1 [:a :b :c]
        l2 [:a :b :c :a :d :a :e]]
    (is (= [[:b :c] 1] (util/remove-first-n l1 2 :a)))
    (is (= [[:b :c :d :a :e] 2] (util/remove-first-n l2 2 :a)))
    (is (= [[:b :c :d :e] 3] (util/remove-first-n l2 10 :a)))
    (is (= [[:a :b :c :a :d :a :e] 0] (util/remove-first-n l2 10 :x)))))

(deftest test-remove-last-n
  (let [l1 [:a :b :c]
        l2 [:a :b :c :a :d :a :e]]
    (is (= [[:b :c] 1] (util/remove-last-n l1 2 :a)))
    (is (= [[:a :b :c :d :e] 2] (util/remove-last-n l2 2 :a)))
    (is (= [[:b :c :d :e] 3] (util/remove-last-n l2 10 :a)))
    (is (= [[:a :b :c :a :d :a :e] 0] (util/remove-last-n l2 10 :x)))))

(deftest test-remove-all
  (let [l1 [:a :b :c :a :d :a :e]]
    (is (= [[:b :c :d :e] 3] (util/remove-all l1 :a)))
    (is (= [[:a :b :c :a :d :a :e] 0] (util/remove-all l1 :x)))))
