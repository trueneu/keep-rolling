(ns keep-rolling-clj.core)

(def default-loop-delay 1)
(def default-loop-retries 5)
(def debug 1)

(defn printlnd [level & more]
  (when (>= debug level)
    (apply println more)))

(defn gen-entity-map [entities-coll]
  (reduce (fn [acc val] (assoc-in acc [(:type val) (:name val)] val)) {} entities-coll))

(def step-data1
  {:type            :step
   :name            :test-step1
   :handler         (fn [params]
                      (println "I am step 1: " (:message params))
                      {:err nil :err-msg nil})
   :required-params [:message]
   :on-failure      :bail})

(def step-data2
  {:type            :step
   :name            :test-step2
   :handler         (fn [params]
                      (println "I am step 2: " (:message params))
                      {:err 1 :err-msg "fucking shit"})
   :required-params [:message]
   :on-failure      :bail})

(def recipe-data1
  {:type :recipe
   :name :test-recipe1
   :steps [:test-step1 :test-step2]
   :handler (fn [params]
              (assoc params :recipe-msg "HELLO SUKA"))})

(def classifier-data1
  {:type :classifier
   :name :test-classifier1
   :required-params [:cluster]
   :handler (fn []
              (cond
                (= :cluster "kafka") ["huyafka"]
                :default ["wowowowa"]))})

(def entity-map (gen-entity-map [step-data1 step-data2 recipe-data1 classifier-data1]))

(def no-err-ret {:err nil :err-msg nil})

(defn nil-or-zero? [arg]
  (or (nil? arg) (zero? arg)))

(defn no-err-ret? [ret]
  (nil-or-zero? (:err ret)))

(defn equal-count? [coll & colls]
  (apply = (count coll) (map count colls)))

(defn extract-params [step params]
  (select-keys params (:required-params step)))

(defn throw-exception-if [predicate ^String msg & params]
  (if (apply predicate params)
    (throw (Exception. msg))
    (last params)))

(defn loop-if [predicate delay retries retry-f f]
  (loop [ret (f)
         count 0]
    (cond
      (not (predicate ret)) ret
      (>= count retries) ret
      :default (do (retry-f ret)
                   (Thread/sleep (* 1000 delay))
                   (recur (f) (inc count))))))

(defn bail-or-skip [step step-ret]
  (let [t (get step :on-failure :bail)]
    (if (no-err-ret? step-ret)
      step-ret
      (cond
        (= t :skip) (do (println (str "Step " (:name step) " failed. Skipping...")) no-err-ret)
        (= t :bail) (do (println (str "Step " (:name step) " failed. Aborting...")) step-ret)))))

(defn make-step-exec-f [step params]
  (fn [] ((:handler step) params)))

(defn println-code-and-msg
  ([ret]
   (println (str "Error code " (:err ret) ": " (:err-msg ret))))
  ([preamble ret]
   (print preamble)
   (println-code-and-msg ret)))

(defn run-step [step params]
  (->> params
       (extract-params step)
       (throw-exception-if (comp not equal-count?) (str "Invalid parameters passed to step " (:name step) ": " params) (:required-params step))
       (make-step-exec-f step)
       (loop-if (comp not no-err-ret?)
                (get step :delay default-loop-delay)
                (get step :retries default-loop-retries)
                #(println-code-and-msg %))
       (bail-or-skip step)))

(defn get-step [step-name]
  (get-in entity-map [:step step-name]))

(defn run-steps [steps params]
  (reduce (fn [acc step]
            (let [run-step-ret (run-step step params)]
              (if (no-err-ret? run-step-ret)
                (conj acc run-step-ret)
                (do (println-code-and-msg "Last error: " run-step-ret)
                    (reduced acc)))))
          []
          steps))

;(run-step (get-step :test-step1) {:message "hello moto"})
;(run-step (get-step :test-step2) {:message "hello moto"})
;(run-steps (map get-step [:test-step1 :test-step2]) {:message "huilo"})
