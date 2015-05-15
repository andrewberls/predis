(defproject com.andrewberls/predis "0.1.0-SNAPSHOT"
  :description "An in-memory Redis mock for Clojure"
  :url "https://github.com/andrewberls/predis"
  :license {:name "MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.taoensso/carmine "2.10.0"]
                 [org.clojure/test.check "0.7.0"]]
  :profiles {:uberjar {:aot :all}})
