(ns keep-rolling-clj.core
  (:require [clojure.test :refer [is]])
  (:require [clojure.pprint :refer [pprint]]))
(def default-loop-delay 1)
(def default-loop-retries 2)
(def debug 1)

(defn printlnd [level & more]
  (when (>= debug level)
    (apply println more)))

(defn gen-entity-map [entities-coll]
  (reduce (fn [acc val] (assoc-in acc [(:type val) (:name val)] val)) {} entities-coll))

(def step-data1
  {:type            :step
   :name            :test-step1
   :action-type     :handler
   :handler         (fn [params]
                      (println "I am step 1: " (:message params))
                      {:err nil :err-msg nil})
   :required-params [:message]
   :on-failure      :bail})

(def step-data2
  {:type            :step
   :name            :test-step2
   :action-type     :start
   :on-failure      :bail})

(def step-data3
  {:type            :step
   :name            :test-step3
   :action-type     :stop
   :on-failure      :skip})

(def recipe-data1
  {:type :recipe
   :name :test-recipe1
   :steps [:test-step1 :test-step2 :test-step3]
   :handler (fn [params]
              {:recipe-msg "HELLO SUKA"})})

(def classifier-data1
  {:type :classifier
   :name :test-classifier1
   :required-params [:cluster]
   :handler (fn [params]
              (cond
                (= (:cluster params) "kafka") ["huyafka"]))})

(def service-data1
  {:type :service
   :name :test-service1
   :required-params [:cluster]
   :matcher (fn [params]
              (= (:cluster params) "kafka"))
   :start (fn [params]
            (println "Service started")
            {:err nil :err-msg nil})
   :stop (fn [params]
            (println "Service stopped")
            {:err 1 :err-msg "couldn't stop service"})})


(def entity-map (gen-entity-map [step-data1 step-data2 step-data3 recipe-data1 classifier-data1 service-data1]))

(def no-err-ret {:err nil :err-msg nil})

(defn nil-or-zero? [arg]
  (or (nil? arg) (zero? arg)))

(defn no-err-ret? [ret]
  (nil-or-zero? (:err ret)))

(defn equal-count? [coll & colls]
  (apply = (count coll) (map count colls)))

(defn extract-params [entity params]
  (select-keys params (:required-params entity)))

(defn throw-exception-if [predicate ^String msg & params]
  (if (apply predicate params)
    (throw (Exception. msg))
    (last params)))

(defn extract-and-check-params [entity params]
  (let [extracted-params (extract-params entity params)
        required-params (:required-params entity)]
    (throw-exception-if (comp not equal-count?) (str "Invalid parameters passed to " (:name entity) ": " params ", wanted: " required-params) required-params extracted-params)
    extracted-params))

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

(defn classify [classifier params]
  (let [classifier-res ((:handler classifier) params)]
    (throw-exception-if empty? (str "Classifier " classifier " did not return anything when called with " params) classifier-res)))

(defn println-code-and-msg
  ([ret]
   (println (str "Error code " (:err ret) ": " (:err-msg ret))))
  ([preamble ret]
   (print preamble)
   (println-code-and-msg ret)))

(defn run-step [step params]
  (->> params
       (extract-and-check-params step)
       (make-step-exec-f step)
       (loop-if (comp not no-err-ret?)
                (get step :delay default-loop-delay)
                (get step :retries default-loop-retries)
                #(println-code-and-msg %))
       (bail-or-skip step)))

(defn get-entity [entity-type entity-name]
  (get-in entity-map [entity-type entity-name]))

(defn get-step [step-name]
  (get-entity :step step-name))

(defn get-classifier [classifier-name]
  (get-entity :classifier classifier-name))

(defn get-service [service-name]
  (get-entity :service service-name))

(defn get-recipe [recipe-name]
  (get-entity :recipe recipe-name))

(defn run-steps [steps params]
  (reduce (fn [acc step]
            (let [run-step-ret (run-step step params)]
              (if (no-err-ret? run-step-ret)
                (conj acc run-step-ret)
                (do (println-code-and-msg "Last error: " run-step-ret)
                    (-> acc (conj run-step-ret) reduced)))))
          []
          steps))

(defn run-classifier [classifier params]
  (->> params
       (extract-and-check-params classifier)
       (classify classifier)))

(defn services-vec []
  (mapv get-service (keys (:service entity-map))))

(defn get-matching-services [services params]
  (filter (fn [service] ((:matcher service) params)) services))

(defn service-to-step [step service]
  (let [action-type (:action-type step)]
    (-> step
        (assoc :action-type :handler)
        (assoc :handler (action-type service)))))

(defn expand-service-step [step services]
  (let [action-type (:action-type step)]
    (if (= action-type :handler)
      [step]
      (map (partial service-to-step step) services))))

(defn expand-service-steps [steps services]
  (reduce
    (fn [acc val]
      (concat acc (expand-service-step val services)))
    []
    steps))

(defn expand-params [params]
  (let [{classifier-name :classifier recipe-name :recipe} params
        classifier (get-classifier classifier-name)
        recipe (get-recipe recipe-name)
        steps (map get-step (:steps recipe))
        params-recipe-addition ((:handler recipe) params)
        params-recipe-enriched (merge params params-recipe-addition)
        hosts (run-classifier classifier params-recipe-enriched)
        params-with-hosts (assoc params-recipe-enriched :hosts hosts)
        matching-services (get-matching-services (services-vec) params-with-hosts)
        expanded-steps (expand-service-steps steps matching-services)
        expanded-params (assoc params-recipe-enriched :steps expanded-steps)]
    expanded-params))

(defn run [params]
  (let [expanded-params (expand-params params)]
    (pprint expanded-params)
    (run-steps (:steps expanded-params) expanded-params)))

;(run {:classifier :test-classifier1
;      :cluster "kafka"
;      :recipe :test-recipe1
;      :message "hui"})

