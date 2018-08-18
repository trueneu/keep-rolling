(ns keep-rolling-clj.utils
  (:require [keep-rolling-clj.protocols])
  (:import (clojure.lang Reflector PersistentVector)))

(def special-fields [:err :err-msg])
(def call-field :call-chain)

(defn plugin-by-name [plugins-map plugin-name]
  (plugins-map (symbol plugin-name)))

(defn call-wrapper [f state]
  (let [retval (f state)
        special-retval (select-keys retval special-fields)
        special-defaulted-retval (merge (apply hash-map (interleave special-fields (repeat nil))) special-retval)
        dissocced-retval (apply dissoc retval special-fields)
        f-name (str f)
        state-with-retval (reduce (fn [acc [k v]] (assoc acc k (conj (acc k) v))) state special-defaulted-retval)
        state-with-retval-and-call-chain (assoc state-with-retval call-field (conj (state-with-retval call-field) f-name))]
    (merge state-with-retval-and-call-chain dissocced-retval)))



;(defn plugins-by-names [plugins-map protocol plugin-names]
;  (apply hash-map (interleave plugin-names (mapcat #(plugin-by-name plugins-map protocol %) plugin-names))))

;(defn plugins-by-names [plugins-map protocol plugin-names]
;  (map #(plugin-by-name plugins-map protocol %) plugin-names))
;
;(defn make-plugin-record
;  ([plugin-class ^PersistentVector args]
;   (Reflector/invokeConstructor (resolve (symbol (.getName plugin-class))) (to-array args)))
;  ([plugin-class]
;   (Reflector/invokeConstructor (resolve (symbol (.getName plugin-class))) (to-array []))))

