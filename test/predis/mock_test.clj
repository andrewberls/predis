(ns predis.mock-test
  (:require [clojure.test :refer :all]
            (predis
              [core :as r]
              [mock :as mock])))

; Keys
(deftest test-del
  (let [redis (mock/->redis {"foo" 1 "bar" 2 "quux" 3 "norf" 4})]
    (is (= 1 (r/del redis "foo")))
    (is (= {"bar" 2 "quux" 3 "norf" 4} @(.store redis)))
    (is (= 2 (r/del redis ["bar" "quux" "fake-1" "fake-2"])))
    (is (= {"norf" 4} @(.store redis)))))

(deftest test-exists
  (let [redis (mock/->redis {"foo" 1})]
    (is (= 1 (r/exists redis "foo")))
    (is (= 0 (r/exists redis "fake-key")))))

(deftest test-keys
  (let [redis (mock/->redis {"foo" 1 "far" 2 "bar" 3})]
    (is (= ["bar" "far" "foo"] (sort (r/keys redis "*"))))
    (is (= ["far" "foo"] (sort (r/keys redis "f*"))))
    (is (= ["bar" "far"] (sort (r/keys redis "*ar"))))))

(deftest test-rename
  (let [redis (mock/->redis {"foo" "1"})]
    (r/rename redis "foo" "bar")
    (is (= {"bar" "1"} @(.store redis)))
    (is (thrown? AssertionError (r/rename redis "bar" "bar")))))

(deftest test-renamenx
  (let [redis (mock/->redis {"foo" "1" "bar" "2"})]
    (is (= 0 (r/renamenx redis "foo" "bar")))
    (is (= {"foo" "1" "bar" "2"} @(.store redis)))
    (is (= 1 (r/renamenx redis "foo" "quux")))
    (is (= {"quux" "1" "bar" "2"} @(.store redis)))
    (is (thrown? AssertionError (r/rename redis "quux" "quux")))))

(deftest test-type
  (let [redis (mock/->redis {"foo" "1" "bar" ["1" "2"] "quux" #{1 2} "norf" {"a" "b"}})]
    (is (= "string" (r/type redis "foo")))
    (is (= "list" (r/type redis "bar")))
    (is (= "set" (r/type redis "quux")))
    (is (= "hash" (r/type redis "norf")))))

(deftest test-scan
  (let [redis (mock/->redis {"foo" 1 "far" 2 "bar" 3 "quux" 4})
        cursor "0"]
    (is (= [cursor ["foo" "bar" "far" "quux"]] (r/scan redis cursor)))
    (is (= [cursor ["foo" "bar" "far" "quux"]] (r/scan redis cursor {:match "*"})))
    (is (= [cursor ["foo" "far"]] (r/scan redis cursor {:match "f*"})))
    (is (= [cursor ["bar" "far"]] (r/scan redis cursor {:match "*ar"})))
    (is (= [cursor ["foo"]] (r/scan redis cursor {:match "*" :count 1})))))

; Server
(deftest test-flushdb
  (let [redis (mock/->redis {"foo" 1 "bar" 2})]
    (is (= {"foo" 1 "bar" 2} @(.store redis)))
    (r/flushdb redis)
    (is (= {} @(.store redis)))))

; Strings
(deftest test-append
  (let [redis (mock/->redis {"foo" "bar"})]
    (is (= {"foo" "bar"} @(.store redis)))
    (is (= 6 (r/append redis "foo" "baz")))
    (is (= {"foo" "barbaz"} @(.store redis)))))

(deftest test-decr
  (let [redis (mock/->redis {"foo" "2"})]
    (is (= 1 (r/decr redis "foo")))
    (is (= {"foo" "1"} @(.store redis)))
    (is (= -1 (r/decr redis "new-key")))
    (is (= {"foo" "1" "new-key" "-1"} @(.store redis)))))

(deftest test-decrby
  (let [redis (mock/->redis {"foo" "5"})]
    (is (= 1 (r/decrby redis "foo" 4)))
    (is (= {"foo" "1"} @(.store redis)))
    (is (= -10 (r/decrby redis "new-key" 10)))
    (is (= {"foo" "1" "new-key" "-10"} @(.store redis)))))

(deftest test-get
  (let [redis (mock/->redis {"bar" "baz"})]
    (is (= "baz" (r/get redis "bar")))
    (is (= nil (r/get redis "fake-key")))))

(deftest test-incr
  (let [redis (mock/->redis {"foo" "2"})]
    (r/incr redis "foo")
    (is (= {"foo" "3"} @(.store redis)))
    (r/incr redis "new-key")
    (is (= {"foo" "3" "new-key" "1"} @(.store redis)))))

(deftest test-incrby
  (let [redis (mock/->redis {"foo" "5"})]
    (is (= 9 (r/incrby redis "foo" 4)))
    (is (= {"foo" "9"} @(.store redis)))
    (is (= 10 (r/incrby redis "new-key" 10)))
    (is (= {"foo" "9" "new-key" "10"} @(.store redis)))))

(deftest test-incrbyfloat
  (let [redis (mock/->redis {"foo" "5"})]
    (is (= "9.3" (r/incrbyfloat redis "foo" 4.3)))
    (is (= {"foo" "9.3"} @(.store redis)))))

(deftest test-mget
  (let [redis (mock/->redis {"foo" 1 "bar" 2 "quux" 3})]
    (is (= [1 2 3] (r/mget redis ["foo" "bar" "quux"])))
    (is (= [1 3] (r/mget redis ["foo" "quux"])))
    (is (= [2 nil] (r/mget redis ["bar" "fake-1"])))
    (is (= [nil nil nil] (r/mget redis ["fake-1" "fake-2" "fake-3"])))))

(deftest test-mset
  (let [redis (mock/->redis {})]
    (r/mset redis [["foo" 1] ["bar" 2]])
    (is (= {"foo" "1" "bar" "2"} @(.store redis)))
    (r/mset redis [["bar" 3] ["quux" 4]])
    (is (= {"foo" "1" "bar" "3" "quux" "4"} @(.store redis)))
    (is (thrown? AssertionError (r/mset redis ["a" 1 "b"])))))

(deftest test-set
  (let [redis (mock/->redis)]
    (r/set redis "foo" "bar")
    (is (= {"foo" "bar"} @(.store redis)))))

(deftest test-strlen
  (let [redis (mock/->redis {"foo" "test" "bar" [1 2]})]
    (is (= 4 (r/strlen redis "foo")))
    (is (= 0 (r/strlen redis "fake-key")))
    (is (thrown? AssertionError (r/strlen redis "bar")))))

; Hashes
(deftest test-hdel
  (let [redis (mock/->redis {"foo" {"bar" 1 "baz" 2 "quux" 3 "norf" 4}})]
    (is (= 1 (r/hdel redis "foo" "bar")))
    (is (= {"foo" {"baz" 2 "quux" 3 "norf" 4}} @(.store redis)))
    (is (= 2 (r/hdel redis "foo" ["baz" "quux" "fake-1" "fake-2"])))
    (is (= {"foo" {"norf" 4}} @(.store redis)))
    (is (= 0 (r/hdel redis "foo" "fake-field")))
    (is (= {"foo" {"norf" 4}} @(.store redis)))
    (is (= 0 (r/hdel redis "fake-key" "fake-field")))
    (is (= {"foo" {"norf" 4}} @(.store redis)))))

(deftest test-hexists
  (let [redis (mock/->redis {"foo" {"bar" 1}})]
    (is (= 1 (r/hexists redis "foo" "bar")))
    (is (= 0 (r/hexists redis "foo" "fake-field")))
    (is (= 0 (r/hexists redis "fake-key" "fake-field")))))

(deftest test-hget
  (let [redis (mock/->redis {"foo" {"bar" 1 "quux" 2}})]
    (is (= 1 (r/hget redis "foo" "bar")))
    (is (= nil (r/hget redis "foo" "fake-field")))
    (is (= nil (r/hget redis "fake-key" "fake-field")))))

(deftest test-hgetall
  (let [redis (mock/->redis {"foo" {"bar" 1 "quux" 2}})]
    (is (= [["bar" 1] ["quux" 2]] (sort (r/hgetall redis "foo"))))
    (is (= [] (r/hgetall redis "fake-key")))))

(deftest test-hincrby
  (let [redis (mock/->redis {"foo" {"bar" "1"}})]
    (is (= 4 (r/hincrby redis "foo" "bar" 3)))
    (is (= {"foo" {"bar" "4"}} @(.store redis)))

    (is (= -5 (r/hincrby redis "foo" "new-field" -5)))
    (is (= {"foo" {"bar" "4" "new-field" "-5"}} @(.store redis)))

    (is (= 10 (r/hincrby redis "new-key" "norf" 10)))
    (is (= {"foo" {"bar" "4" "new-field" "-5"} "new-key" {"norf" "10"}} @(.store redis)))))

(deftest test-hincrbyfloat
  (let [redis (mock/->redis {"foo" {"bar" "5"}})]
    (is (= 9.3 (r/hincrbyfloat redis "foo" "bar" 4.3)))
    (is (= {"foo" {"bar" "9.3"}} @(.store redis)))))

(deftest test-hlen
  (let [redis (mock/->redis {"foo" {"bar" 1 "quux" 2}})]
    (is (= 2 (r/hlen redis "foo")))
    (is (= 0 (r/hlen redis "fake-key")))))

(deftest test-hmget
  (let [redis (mock/->redis {"foo" {"bar" 1 "baz" 2}})]
    (is (= [1] (r/hmget redis "foo" "bar")))
    (is (= [1 2] (r/hmget redis "foo" ["bar" "baz"])))
    (is (= [2 nil] (r/hmget redis "foo" ["baz" "fake"])))
    (is (= [nil] (r/hmget redis "foo" ["fake"])))
    (is (= [nil] (r/hmget redis "foo" "fake")))))

(deftest test-hmset
  (let [redis (mock/->redis {"foo" {"bar" 1}})]
    (r/hmset redis "foo" [["baz" 2]])
    (is (= {"foo" {"bar" 1 "baz" 2}} @(.store redis)))
    (r/hmset redis "new-key" [["quux" 3]])
    (is (= {"foo" {"bar" 1 "baz" 2} "new-key" {"quux" 3}} @(.store redis)))))

(deftest test-hvals
  (let [redis (mock/->redis {"foo" {"quux" 2 "bar" 1}})]
    (is (= [1 2] (r/hvals redis "foo")))
    (is (= [] (r/hvals redis "fake-key")))))

; Lists
(deftest test-llen
  (let [redis (mock/->redis {"foo" ["1" "2" "3"]})]
    (is (= 3 (r/llen redis "foo")))
    (is (= 0 (r/llen redis "fake-key")))))

(deftest test-lpop
  (let [redis (mock/->redis {"foo" ["1" "2"]})]
    (is (= "1" (r/lpop redis "foo")))
    (is (= {"foo" ["2"]} @(.store redis)))
    (is (= "2" (r/lpop redis "foo")))
    (is (= {} @(.store redis)))
    (is (= nil (r/lpop redis "foo")))
    (is (= {} @(.store redis)))
    (is (= nil (r/lpop redis "fake-key")))))

(deftest test-lpush
  (let [redis (mock/->redis {"foo" ["1" "2" "3"]})]
    (is (= 4 (r/lpush redis "foo" [4])))
    (is (= {"foo" ["4" "1" "2" "3"]} @(.store redis)))
    (is (= 5 (r/lpush redis "foo" 5)))
    (is (= {"foo" ["5" "4" "1" "2" "3"]} @(.store redis)))
    (r/lpush redis "new-key" ["1" "2" "3"])
    (is (= {"foo" ["5" "4" "1" "2" "3"] "new-key" ["3" "2" "1"]} @(.store redis)))))

(deftest test-lrange
  (let [redis (mock/->redis {"foo" ["1" "2" "3" "4" "5" "6" "7" "8"]})]
    (is (= ["1" "2" "3"] (r/lrange redis "foo" 0 2)))
    (is (= ["1" "2" "3" "4" "5" "6" "7" "8"] (r/lrange redis "foo" 0 100)))
    (is (= [] (r/lrange redis "foo" 100 101)))
    (is (= ["1" "2" "3" "4" "5" "6" "7" "8"] (r/lrange redis "foo" 0 -1)))
    (is (= ["7" "8"] (r/lrange redis "foo" -2 -1)))
    (is (= [] (r/lrange redis "foo" -2 -3)))
    (is (= ["1" "2" "3" "4" "5" "6" "7"] (r/lrange redis "foo" 0 -2)))
    (is (= [] (r/lrange redis "fake-key" 0 10)))))

(deftest test-lrem
  (let [redis (mock/->redis {"foo" ["1" "2" "3" "1" "4" "1" "3"]})]
    (is (= 2 (r/lrem redis "foo" 2 1)))
    (is (= {"foo" ["2" "3" "4" "1" "3"]} @(.store redis)))
    (is (= 1 (r/lrem redis "foo" 10 1)))
    (is (= {"foo" ["2" "3" "4" "3"]} @(.store redis)))
    (is (= 1 (r/lrem redis "foo" 0 4)))
    (is (= {"foo" ["2" "3" "3"]} @(.store redis)))
    (is (= 1 (r/lrem redis "foo" -1 3)))
    (is (= {"foo" ["2" "3"]} @(.store redis)))
    (is (= 0 (r/lrem redis "fake-key" 10 1)))))

(deftest test-rpop
  (let [redis (mock/->redis {"foo" ["1" "2"]})]
    (is (= "2" (r/rpop redis "foo")))
    (is (= {"foo" ["1"]} @(.store redis)))
    (is (= "1" (r/rpop redis "foo")))
    (is (= {} @(.store redis)))
    (is (= nil (r/rpop redis "foo")))
    (is (= {} @(.store redis)))
    (is (= nil (r/rpop redis "fake-key")))))

(deftest test-rpush
  (let [redis (mock/->redis {"foo" ["1" "2" "3"]})]
    (is (= 4 (r/rpush redis "foo" [4])))
    (is (= {"foo" ["1" "2" "3" "4"]} @(.store redis)))
    (is (= 5 (r/rpush redis "foo" 5)))
    (is (= {"foo" ["1" "2" "3" "4" "5"]} @(.store redis)))
    (r/rpush redis "new-key" ["1" "2" "3"])
    (is (= {"foo" ["1" "2" "3" "4" "5"] "new-key" ["1" "2" "3"]} @(.store redis)))))

; Sets
(deftest test-sadd
  (let [redis (mock/->redis {"foo" #{"1" "2"}})]
    (is (= 1 (r/sadd redis "foo" "3")))
    (is (= {"foo" #{"1" "2" "3"}} @(.store redis)))
    (is (= 2 (r/sadd redis "foo" ["3" "4" "5"])))
    (is (= {"foo" #{"1" "2" "3" "4" "5"}} @(.store redis)))
    (r/sadd redis "new-key" ["1" "2"])
    (is (= {"foo" #{"1" "2" "3" "4" "5"} "new-key" #{"1" "2"}} @(.store redis)))))

(deftest test-scard
  (let [redis (mock/->redis {"foo" #{"1" "2"}})]
    (is (= 2 (r/scard redis "foo")))
    (is (= 0 (r/scard redis "fake-key")))))

(deftest test-sdiff
  (let [redis (mock/->redis {"foo" #{"1" "2" "3"} "bar" #{"2" "3" "4"} "quux" #{"4" "5" "6"}})]
    (is (= ["1" "2" "3"] (sort (r/sdiff redis "foo"))))
    (is (= ["1"] (r/sdiff redis ["foo" "bar" "quux"])))
    (is (= ["1"] (r/sdiff redis ["foo" "fake-set" "bar"])))
    (is (= [] (r/sdiff redis ["fake-set" "foo" "bar"])))))

(deftest test-sdiffstore
  (let [redis (mock/->redis {"foo" #{"1" "2" "3"} "bar" #{"2" "3" "4"}})]
    (is (= 1 (r/sdiffstore redis "norf" ["foo" "bar"])))
    (is (= {"foo" #{"1" "2" "3"} "bar" #{"2" "3" "4"} "norf" #{"1"}} @(.store redis)))))

(deftest test-sinter
  (let [redis (mock/->redis {"foo" #{"1" "2" "3"} "bar" #{"2" "3" "4"} "quux" #{"4" "5" "6"}})]
    (is (= ["1" "2" "3"] (sort (r/sinter redis "foo"))))
    (is (= ["2" "3"] (sort (r/sinter redis ["foo" "bar"]))))
    (is (= [] (sort (r/sinter redis ["foo" "bar" "quux"]))))
    (is (= [] (r/sinter redis ["foo" "fake-set" "bar"])))
    (is (= [] (r/sinter redis ["fake-set" "foo" "bar"])))))

(deftest test-sinterstore
  (let [redis (mock/->redis {"foo" #{"1" "2" "3"} "bar" #{"2" "3" "4"}})]
    (is (= 2 (r/sinterstore redis "norf" ["foo" "bar"])))
    (is (= {"foo" #{"1" "2" "3"} "bar" #{"2" "3" "4"} "norf" #{"2" "3"}} @(.store redis)))))

(deftest test-sismember
  (let [redis (mock/->redis {"foo" #{"1" "2" "3"}})]
    (is (= 1 (r/sismember redis "foo" 1)))
    (is (= 0 (r/sismember redis "foo" 99)))
    (is (= 0 (r/sismember redis "bar" 1)))))

(deftest test-smembers
  (let [redis (mock/->redis {"foo" #{"1" "2" "3"}})]
    (is (= ["1" "2" "3"] (sort (r/smembers redis "foo"))))
    (is (= [] (sort (r/smembers redis "fake-key"))))))

(deftest test-smove
  (let [redis (mock/->redis {"foo" #{"1" "2" "3"} "bar" #{"4"}})]
    (is (= 1 (r/smove redis "foo" "bar" "3")))
    (is (= {"foo" #{"1" "2"} "bar" #{"3" "4"}} @(.store redis)))
    (is (= 0 (r/smove redis "foo" "bar" "99")))
    (is (= {"foo" #{"1" "2"} "bar" #{"3" "4"}} @(.store redis)))
    (is (= 1 (r/smove redis "foo" "quux" "2")))
    (is (= {"foo" #{"1"} "bar" #{"3" "4"} "quux" #{"2"}} @(.store redis)))))

(deftest test-srem
  (let [redis (mock/->redis {"foo" #{"1" "2" "3" "4" "5"}})]
    (is (= 1 (r/srem redis "foo" "5")))
    (is (= {"foo" #{"1" "2" "3" "4"}} @(.store redis)))
    (is (= 2 (r/srem redis "foo" ["3" "4" "98" "99"])))
    (is (= {"foo" #{"1" "2"}} @(.store redis)))
    (is (= 0 (r/srem redis "fake-key" ["1" "2" "3"])))))

(deftest test-sunion
  (let [redis (mock/->redis {"foo" #{"1" "2" "3"} "bar" #{"2" "3" "4"} "quux" #{"4" "5" "6"}})]
    (is (= ["1" "2" "3"] (sort (r/sunion redis "foo"))))
    (is (= ["1" "2" "3" "4"] (sort (r/sunion redis ["foo" "bar"]))))
    (is (= ["1" "2" "3" "4" "5" "6"] (sort (r/sunion redis ["foo" "bar" "quux"]))))
    (is (= ["1" "2" "3" "4"] (sort (r/sunion redis ["foo" "fake-set" "bar"]))))
    (is (= ["1" "2" "3" "4"] (sort (r/sunion redis ["fake-set" "foo" "bar"]))))))

(deftest test-sunionstore
  (let [redis (mock/->redis {"foo" #{"1" "2" "3"} "bar" #{"2" "3" "4"}})]
    (is (= 4 (r/sunionstore redis "norf" ["foo" "bar"])))
    (is (= {"foo" #{"1" "2" "3"} "bar" #{"2" "3" "4"} "norf" #{"1" "2" "3" "4"}} @(.store redis)))))
