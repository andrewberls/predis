(ns predis.util)

(defn vec-wrap [v-or-vs]
  (if (sequential? v-or-vs) v-or-vs [v-or-vs]))

(defn stringify-tuple [[a b]]
  [(str a) (str b)])

(defn values-at
  "Similar to (comp vals select-keys), but includes nil for missing keys
   Ex:
     (values-at {:foo 1} [:foo :bar])
     [1 nil]"
  [m ks]
  (map (partial get m) ks))

(defn counting-dissoc
  "Similar to dissoc, but counts the number of elements removed
   Returns tuple of [new-map num-items-removed]"
  [m ks]
  (let [do-dissoc (fn [[m' nremoved :as acc] k]
                  (if (get m' k)
                    [(dissoc m' k) (inc nremoved)]
                    acc))
        init [m 0]]
    (reduce do-dissoc init ks)))

(defn counting-disj
  "Similar to disj, but counts the number of elements removed
   Returns tuple of [new-set num-items-removed]"
  [s ks]
  (let [do-disj (fn [[s' nremoved :as acc] k]
                  (if (contains? s' k)
                    [(disj s' k) (inc nremoved)]
                    acc))
        init [s 0]]
    (reduce do-disj init ks)))

(defn counting-union
  "Similar to clojure.set/union, but counts the number of elements added
   Returns tuple of [new-set num-items-added]"
  [s vs]
  (let [do-union (fn [[s' nadded :as acc] v]
                   (if (contains? s' v)
                     [s' nadded]
                     [(conj s' v) (inc nadded)]))
        init [s 0]]
    (reduce do-union init vs)))

; TODO: This code is massively jank

(defn reduce-right
  "Based on https://gist.github.com/kohyama/2893987"
  [f init coll]
  (loop [[c & cs] coll
         rvsd '()]
    (if (nil? c)
        (loop [acc init
               [r & rs] rvsd]
          (if r (recur (f acc r) rs) acc))
        (recur cs (cons c rvsd)))))

(defn remove-first-n
  "Remove the first n occurrences of v from xs, returning
   a tuple of [new-list num-items-removed]"
  [xs n v]
  (let [counting-remove
        (fn [[xs' n'] x]
          (if (and (= x v) (> n' 0))
            [xs' (dec n')]      ; Discard
            [(conj xs' x) n'])) ; Keep
        init [[] n]
        [s' guard] (reduce counting-remove init xs)]
    [s' (- n guard)]))

(defn remove-last-n
  "Remove the last n occurrences of v from xs, returning
   a tuple of [new-list num-items-removed]"
  [xs n v]
  (let [counting-remove
        (fn [[xs' n'] x]
          (if (and (= x v) (> n' 0))
            [xs' (dec n')]      ; Discard
            [(conj xs' x) n'])) ; Keep
        init [[] n]
        [s' guard] (reduce-right counting-remove init xs)]
    [(reverse s') (- n guard)]))

(defn remove-all
  "Remove all occurrencess of v from xs, returning
   a tuple of [new-list num-items-removed]"
  [xs v]
  (let [counting-remove
        (fn [[xs' nremoved :as acc] el]
                      (if (= el v)
                        [xs' (inc nremoved)]
                        [(conj xs' el) nremoved]))
        init [[] 0]]
    (reduce counting-remove init xs)))
