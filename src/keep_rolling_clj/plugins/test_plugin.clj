(ns ^{:kr-type :ns} keep-rolling-clj.plugins.test-plugin
  (:require [keep-rolling-clj.protocols :refer :all]))

(defn ^{:kr-type :step} step1 [state]
  {:err nil :err-msg nil})

(defn ^{:kr-type :recipe} recipe1 [state]
  (let [steps ["step1"]]
    (println "Executing steps" steps)
    (reduce (fn [acc step-name]
              (keep-rolling-clj.utils/call-wrapper
                (keep-rolling-clj.utils/plugin-by-name
                  (state :plugins-map)
                  step-name)
                acc))
            state steps)))




;(defrecord test-step1 []
;  IStep
;  (defn-method run-step [this args]
;    (println "I'm step1 called with" args)))
;
;(defrecord test-step2 []
;  IStep
;  (run-step [this args] (println "I'm step2 called with" args)))
;
;(defrecord test-recipe []
;  IRecipe
;  (run-recipe [this args]
;    (println "Running test recipe with " args)
;    (let [steps ["test-step1" "test-step2"]
;          step-classes (keep-rolling-clj.utils/plugins-by-names args IStep steps)
;          step-records (map keep-rolling-clj.utils/make-plugin-record step-classes)]
;      (println step-records)
;      (reduce (fn [_ rec] (run-step rec {:some :args}))
;              nil step-records))))
;
;(defrecord test-classifier []
;  IClassifier
;  (classify [this args]
;    (let [{:keys [cluster]} args]
;      (cond
;        (= cluster "kafka") {:hostnames ["kafka1.com", "kafka2.com"]
;                             :services ["test-service"]}))))
;
;(defrecord test-service []
;  IService
;  (service-stop [this args]
;    (println "doing a service-stop")
;    {:step "service-stop" :success true :err nil})
;
;  (service-start [this args])
;  (service-restart [this args])
;  (service-status [this args])
;  (application-status [this args]))
