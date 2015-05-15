## Predis

Predis is a Clojure protocol for [Redis](http://redis.io/), which allows for multiple client implementations with a common API.
Most notably Predis provides an in-memory mock client implementation for testing purposes, similar to, and inspired by [brigade/mock_redis](https://github.com/brigade/mock_redis).

### Installation
TODO

### Usage
```clj
(require '[predis.core :as redis])

; Using the in-memory mock
(require '[predis.mock :as mock])
(def mock-client (mock/->redis))
(redis/set mock-client "foo" "bar") ; => "OK"
(redis/get mock-client "foo") ; => "bar"
```
