(ns keep-rolling-clj.protocols)

(defprotocol IStep
  (run-step [this args]))

(defprotocol IRecipe
  (run-recipe [this args]))

(defprotocol IService
  (service-stop [this args])
  (service-start [this args])
  (service-restart [this args])
  (service-status [this args])
  (application-status [this args]))

(defprotocol IClassifier
  (classify [this args]))

(def special-fields [:err :err-msg])
(def method-field :call-chain)

(defmacro defn-method [method-name [this args] & body]
  (let [last-form# (last body)
        first-forms# (butlast body)]
    `(defn ~method-name [~this ~args]
       (do ~@first-forms#
           (let [retval# ~last-form#
                 special-retval# (select-keys retval# special-fields)
                 special-defaulted-retval# (merge (apply hash-map (interleave special-fields (repeat nil))) special-retval#)
                 dissocced-retval# (apply dissoc retval# special-fields)

                 args-with-method-name# (assoc ~args method-field (conj (~args method-field) (str (.getClass ~this) "/" '~method-name)))]
             (merge
               (loop [res# args-with-method-name#
                      dr# special-defaulted-retval#]
                 (if (empty? dr#)
                   res#
                   (let [[k# v#] (first dr#)]
                     (recur (assoc res# k# (conj (res# k#) v#))
                            (rest dr#)))))
               dissocced-retval#))))))

(defmacro def-record [record-name proto method-name [this args] & body]
  `(defrecord ~record-name []
     ~proto
     ~(defn-method method-name [this args] body)))

