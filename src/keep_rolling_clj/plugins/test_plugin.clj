(ns keep-rolling-clj.plugins.test-plugin
  (:require [keep-rolling-clj.utils :as utils]))

(def ^:kr step-data1
  {:type               :step
   :name               :test-step1
   :action-type        :handler
   :handler            (fn [params]
                         (Thread/sleep 1000)
                         (utils/safe-println "blah")
                         {:err 0 :err-msg nil})
   :required-params    [:message :host]
   :on-failure         :bail
   :delay              1
   :parallel-execution false
   :only-once          true
   :doc                "Prints a message"})

(def ^:kr step-data2
  {:type        :step
   :name        :test-step2
   :action-type :start
   :on-failure  :bail
   :retries     2
   :delay       1})

(def ^:kr step-data3
  {:type        :step
   :name        :test-step3
   :action-type :stop
   :on-failure  :skip
   :retries     2
   :delay       1
   :service-priority-sort :descending})

;(def ^:kr recipe-data1
;  {:type    :recipe
;   :name    :test-recipe1
;   :steps   [:test-step2 :test-step3]
;   :handler (fn [params]
;              {:recipe-msg "HELLO SUKA"})})
;
;(def ^:kr classifier-data1
;  {:type            :classifier
;   :name            :test-classifier1
;   :required-params [:cluster]
;   :handler         (fn [params]
;                      (cond
;                        (= (:cluster params) "kafka") ["host2"]))})

(def ^:kr recipe-data1
  {:type    :recipe
   :name    :test-recipe1
   :steps   [[:test-step1] [:test-step2 :test-step3]]
   :handler (fn [params]
              nil)})

(def ^:kr classifier-data1
  {:type            :classifier
   :name            :test-classifier1
   :required-params [:cluster]
   :handler         (fn [params]
                      (cond
                        (= (:cluster params) "kafka") ["host1" "host2"]))})

(def ^:kr service-data1
  {:type            :service
   :name            :test-service1
   :required-params [:cluster :host]
   :matcher         (fn [params]
                      (= (:cluster params) "kafka"))
   :start           (fn [params]
                      (utils/safe-println "Service1 started")
                      (if (= (:host params) "host1")
                        {:err nil :err-msg nil}
                        {:err nil :err-msg nil}))
                        ;{:err 100 :err-msg "because fuck off"}))
   :stop            (fn [params]
                      (utils/safe-println "Service1 stopped")
                      {:err 0 :err-msg "couldn't stop service"})})

(def ^:kr service-data2
  {:type            :service
   :name            :test-service2
   :required-params [:cluster]
   :priority        50
   :matcher         (fn [params]
                      (= (:cluster params) "kafka"))
   :start           (fn [params]
                      (utils/safe-println "Service2 started")
                      {:err nil :err-msg nil})
   :stop            (fn [params]
                      (utils/safe-println "Service2 stopped")
                      {:err 0 :err-msg "couldn't stop service"})})
