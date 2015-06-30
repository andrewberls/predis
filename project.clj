(defproject com.andrewberls/predis "0.1.4"
  :description "An in-memory Redis mock for Clojure"
  :url "https://github.com/andrewberls/predis"
  :license {:name "MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.taoensso/carmine "2.11.1"]
                 [org.clojure/test.check "0.7.0"]]
  :profiles {:uberjar {:aot :all}}
  :plugins [[codox "0.8.12"]]
  :codox {:src-dir-uri "http://github.com/andrewberls/predis/tree/master"})
