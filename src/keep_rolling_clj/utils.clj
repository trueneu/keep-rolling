(ns keep-rolling-clj.utils
  (:import (java.io File)))

(def plugins-path "/Users/pgurkov/git_tree/keep-rolling-clj/src/keep_rolling_clj/plugins")

(defn load-plugins []
  (let [all-files (file-seq (File. ^String plugins-path))
        without-dirs (remove #(.isDirectory ^File %) all-files)
        only-clj (filter #(.endsWith (.getName %) ".clj") without-dirs)]
    (doseq [plugin only-clj]
      (load-file (.getPath plugin)))))

(load-plugins)

(defn get-all-plugins []
  (->> (all-ns)
   (mapcat ns-publics)
   (map second)
   (filter #(-> % meta :kr))
   (map deref)))

(defn deep-map
  ([f coll]
   (loop [c coll
          res []]
     (if (or (empty? c))
       res
       (let [[f-c & r-c] c]
         (cond
           (vector? f-c) (recur r-c (conj res (deep-map f f-c)))
           :default (recur r-c (f res f-c))))))))

(defn deep-map-scalar-helper [f]
  (fn [coll x] (conj coll (f x))))

(defn deep-map-coll-helper [f]
  (fn [coll x] ((comp vec concat) coll (f x))))

(defn deep-map-scalar [f coll]
  (deep-map (deep-map-scalar-helper f) coll))

(defn deep-map-coll [f coll]
  (deep-map (deep-map-coll-helper f) coll))

(defn same-type? [coll]
  (apply = (map type coll)))

(defn deep-same-type [coll]
  (let [same-type (same-type? coll)]
    (reduce
      (fn [acc val]
        (if (vector? val)
          (and acc (deep-same-type val))
          acc))
      same-type
      coll)))

