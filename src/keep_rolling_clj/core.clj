(ns keep-rolling-clj.core
  (:require [keep-rolling-clj.utils :as u])
  (:require [clojure.test :refer [is]])
  (:require [clojure.pprint :refer [pprint]])
  (:import [Double]))
(def debug 999)


(def default-step-params
  {:on-failure         :bail
   :delay              5
   :retries            Double/POSITIVE_INFINITY
   :parallel-execution false})


(defn gen-entity-map [entities-coll]
  (reduce (fn [acc val] (assoc-in acc [(:type val) (:name val)] val)) {} entities-coll))


(def entity-map (gen-entity-map (u/get-all-plugins)))
(def no-err-ret {:err nil :err-msg nil})


(defn print-debug [level & more]
  (when (>= debug level)
    (doseq [object more]
      (cond
        (= String (type object)) (println object)
        :default (pprint object)))))


(defn extract-params [entity params]
  (select-keys params (:required-params entity)))


(defn throw-exception-if [predicate ^String msg & coll]
  (if (apply predicate coll)
    (throw (Exception. msg))
    (last coll)))


(defn extract-and-check-params [entity params]
  (let [extracted-params (extract-params entity params)
        required-params (:required-params entity)]
    (throw-exception-if (comp not u/equal-count?) (str "Invalid parameters passed to " (:name entity) ": " params ", wanted: " required-params) required-params extracted-params)
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
    (if (u/no-err-ret? step-ret)
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
    (print-debug 1 (str "Running step " (:name step) (if (:service step) (str ", service " (:service step)) "") " on host " (:host params)))
    (->> params
         (extract-and-check-params step)
         (make-step-exec-f step)
         (loop-if (comp not u/no-err-ret?)
                  (:delay step)
                  (:retries step)
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

  ;(loop [hosts (:hosts params)]
  ;  (let [[host & rest-hosts] hosts]
  ;    (if (empty? hosts)
  ;      no-err-ret
  ;      (if (no-err-ret?     (loop [curr-steps steps]
  ;                             (let [[step & rest-steps] curr-steps
  ;                                   step-ret (run-step step (assoc params :host host))]
  ;                               ;(print-debug 1 "Current steps:" curr-steps)
  ;                               (if (no-err-ret? step-ret)
  ;                                 (if (empty? rest-steps)
  ;                                   no-err-ret
  ;                                   (recur rest-steps))
  ;                                 (println-code-and-msg "Last error: " step-ret)))))
  ;        (recur (rest hosts))
  ;        {:err :err})))))


(defn run-step-group-on-host [steps host params]
  (let [params-with-host (assoc params :host host)]
    (reduce (fn [_ step]
              (let [run-step-ret (run-step step params-with-host)]
                (when-not (u/no-err-ret? run-step-ret)
                  (println-code-and-msg "Last error: " run-step-ret)
                  (reduced run-step-ret))))
      no-err-ret
      steps)))


(defn run-step-group-on-hosts [steps params]
  (reduce (fn [_ host]
            (let [run-step-group-ret (run-step-group-on-host steps host params)]
              (when-not (u/no-err-ret? run-step-group-ret)
                (reduced run-step-group-ret))))
    no-err-ret
    (:hosts params)))

;; todo above two look very similar


(defn run-step-group [steps params]
  (loop [s steps
         hosts (:hosts params)
         prev-ret no-err-ret]
    (if (or (empty? hosts) (u/err-ret? prev-ret))
      prev-ret
      (let [[f-s & r-s] s]
        (cond
          (vector? f-s) (recur r-s hosts (run-step-group-on-hosts f-s params))
          :default (run-step-group-on-hosts s params))))))


(defn run-classifier [classifier params]
  (->> params
       (extract-and-check-params classifier)
       (classify classifier)))


(defn services-vec []
  (->> entity-map
       (:service)
       (keys)
       (mapv get-service)))


(defn get-matching-services [services params]
  (filter (fn [service] ((:matcher service) params)) services))


(defn service-to-step [step service]
  (let [action-type (:action-type step)]
    (-> step
        (assoc :action-type :handler)
        (assoc :handler (action-type service))
        (assoc :service (:name service)))))


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

;(nested-fn get-step [[:test-step1 :test-step2] :test-step3 [:test-step1]])
;(deep-map
;  (fn [coll x] (conj coll (get-step x)))
;  [[:test-step1 :test-step2] :test-step3 [:test-step1]])

(defn check-steps-nesting [steps]
  (throw-exception-if
    (comp not u/deep-same-type)
    (str "Steps collection must not have dangling steps: " steps)
    steps))


(defn expand-params [params]
  (let [{classifier-name :classifier recipe-name :recipe} params
        classifier (get-classifier classifier-name)
        recipe (get-recipe recipe-name)
        params-recipe-addition ((:handler recipe) params)
        params-recipe-enriched (merge params params-recipe-addition)
        hosts (run-classifier classifier params-recipe-enriched)
        params-with-hosts (assoc params-recipe-enriched :hosts hosts)
        matching-services (get-matching-services (services-vec) params-with-hosts)

        steps (->> (:steps recipe) (check-steps-nesting) (u/deep-map-scalar get-step) (u/deep-map-scalar #(merge default-step-params %)))
        expanded-steps (u/deep-map-coll #(expand-service-step % matching-services) steps)
        expanded-params (assoc params-with-hosts :steps expanded-steps)]

    expanded-params))


(defn run [params]
  (let [expanded-params (expand-params params)]
    (print-debug 1 expanded-params)
    (run-step-group (:steps expanded-params) expanded-params)))


(run {:classifier :test-classifier1
      :cluster "kafka"
      :recipe :test-recipe1
      :message "hui"})

