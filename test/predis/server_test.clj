(ns predis.server-test
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

(defspec test-dbsize
  test-utils/nruns
  (let [mock-client (mock/->redis)]
    (prop/for-all [k1 (gen/not-empty gen/string-alphanumeric)
                   v1 gen/string-alphanumeric
                   k2 (gen/not-empty gen/string-alphanumeric)
                   v2 gen/string-alphanumeric]
      (test-utils/assert-set mock-client carmine-client k1 v1)
      (test-utils/assert-set mock-client carmine-client k2 v2)
      (is (= (r/dbsize mock-client) (r/dbsize carmine-client))))))
