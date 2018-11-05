(ns keep-rolling-clj.plugins.test-plugin)

(def ^:kr step-data1
  {:type               :step
   :name               :test-step1
   :action-type        :handler
   :handler            (fn [params]
                         (Thread/sleep 1000)
                         {:err 0 :err-msg nil})
   :required-params    [:message :host]
   :on-failure         :bail
   :retries            2
   :delay              1
   :parallel-execution true})

(def ^:kr step-data2
  {:type        :step
   :name        :test-step2
   :action-type :start-prepare
   :on-failure  :bail
   :retries     2
   :delay       1})

(def ^:kr step-data3
  {:type        :step
   :name        :test-step3
   :action-type :stop
   :on-failure  :skip
   :retries     2
   :delay       1})

(def ^:kr recipe-data1
  {:type    :recipe
   :name    :test-recipe1
   :steps   [[:test-step1] [:test-step2 :test-step3]]
   :handler (fn [params]
              {:recipe-msg "HELLO SUKA"})})

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
   :required-params [:cluster]
   :matcher         (fn [params]
                      (= (:cluster params) "kafka"))
   :start           (fn [params]
                      (println "Service1 started")
                      {:err nil :err-msg nil})
   :stop            (fn [params]
                      (println "Service1 stopped")
                      {:err 0 :err-msg "couldn't stop service"})})

(def ^:kr service-data2
  {:type            :service
   :name            :test-service2
   :required-params [:cluster]
   :matcher         (fn [params]
                      (= (:cluster params) "kafka"))
   :start           (fn [params]
                      (println "Service2 started")
                      {:err nil :err-msg nil})
   :stop            (fn [params]
                      (println "Service2 stopped")
                      {:err 0 :err-msg "couldn't stop service"})})
