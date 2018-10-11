(ns keep-rolling-clj.plugins.test-plugin)

(def ^:kr plugin-step1
  {:type            :step
   :name            :plugin-step1
   :action-type     :handler
   :handler         (fn [params]
                      (println "I am step 1: " (:message params))
                      {:err nil :err-msg nil})
   :required-params [:message]
   :on-failure      :bail})

(def ^:kr step-data1
  {:type            :step
   :name            :test-step1
   :action-type     :handler
   :handler         (fn [params]
                      (println "I am step 1: " (:message params))
                      {:err nil :err-msg nil})
   :required-params [:message]
   :on-failure      :bail})

(def ^:kr step-data2
  {:type            :step
   :name            :test-step2
   :action-type     :start
   :on-failure      :bail})

(def ^:kr step-data3
  {:type            :step
   :name            :test-step3
   :action-type     :stop
   :on-failure      :skip})

(def ^:kr recipe-data1
  {:type :recipe
   :name :test-recipe1
   :steps [:test-step1 :test-step2 :test-step3]
   :handler (fn [params]
              {:recipe-msg "HELLO SUKA"})})

(def ^:kr classifier-data1
  {:type :classifier
   :name :test-classifier1
   :required-params [:cluster]
   :handler (fn [params]
              (cond
                (= (:cluster params) "kafka") ["huyafka"]))})

(def ^:kr service-data1
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
