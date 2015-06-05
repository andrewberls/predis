(ns predis.keys-test
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            (clojure.test.check
              [clojure-test :refer [defspec]]
              [generators :as gen]
              [properties :as prop])
            [taoensso.carmine :as carmine]
            (predis
              [core :as r]
              [mock :as mock])
            [predis.test-utils :as test-utils]))

(def carmine-client (test-utils/carmine-client))

(use-fixtures :each test-utils/flush-redis)

(defspec test-del
  test-utils/nruns
  (let [mock-client (mock/->redis)]
    (prop/for-all [k gen/string-alphanumeric
                   v gen/string-alphanumeric]
      (test-utils/assert-set mock-client carmine-client k v)
      (test-utils/dbs-equal mock-client carmine-client)
      (is (= (r/del mock-client k) (r/del carmine-client k)))
      (test-utils/dbs-equal mock-client carmine-client))))

(defspec test-rename
  test-utils/nruns
  (let [mock-client (mock/->redis)]
    (prop/for-all [v gen/string-alphanumeric]
      (let [k1 (str (java.util.UUID/randomUUID))
            k2 (str (java.util.UUID/randomUUID))]
        (test-utils/assert-set mock-client carmine-client k1 v)
        (is (= (r/rename mock-client k1 k2) (r/rename carmine-client k1 k2)))
        (test-utils/dbs-equal mock-client carmine-client)))))
