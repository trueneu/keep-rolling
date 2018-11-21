(ns keep-rolling-clj.utils
  (:require [clojure.pprint :as pprint])
  (:import (java.io OutputStreamWriter)))

(def no-err-ret {:err nil :err-msg nil})
(def debug 999)
(def stdout-lock (Object.))
(def out *out*)
(def ansi-esc (str (char 27) \[))
(def color-map
  {:black        30
   :red          31
   :green        32
   :yellow       33
   :blue         34
   :magenta      35
   :cyan         36
   :white        37})
(def stage-padding 8)
(def step-padding 20)
(def service-padding 32)


(defn locked [lock fn & more]
  (locking lock
    (apply fn more)))


(defn safe-print [& more]
  (apply locked stdout-lock
          (fn [& m]
            (.write out (str (clojure.string/join " " m)))
            (.flush out))
          more))


(defn safe-println [& more]
  (apply safe-print (conj (vec more) "\n")))


(defn safe-print-debug [level & more]
  (when (>= debug level)
    (safe-print
      (with-out-str
        (doseq [object more]
         (cond
           (= String (type object)) (println object)
           :default (pprint/pprint object)))))))


(defn make-code-and-msg-string
  ([ret]
   (make-code-and-msg-string "" ret))
  ([preamble ret]
   (str preamble "Error code " (:err ret) ": " (:err-msg ret))))


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


(defn nil-or-zero? [arg]
  (or (nil? arg) (zero? arg)))


(defn no-err-ret? [ret]
  (nil-or-zero? (:err ret)))


(defn err-ret? [ret]
  ((comp not no-err-ret?) ret))


(defn remove-no-err-ret [coll]
  (remove #(= % no-err-ret) coll))


(defn equal-count? [coll & colls]
  (apply = (count coll) (map count colls)))


(defn escape [n]
  (let [n (if (sequential? n) (clojure.string/join ";" n) n)]
      (str ansi-esc n "m")))


(defn colorize [color s]
  (str (escape (get color-map color)) s (escape (get color-map :black))))


(defn pad-string [padding s]
  (str s (apply str (repeat (- padding (count s)) " "))))


(defn pad-stage [s]
  (pad-string stage-padding s))


(defn pad-step [s]
  (pad-string step-padding s))


(defn pad-service [s]
  (pad-string service-padding s))
