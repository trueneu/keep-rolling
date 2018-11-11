(ns keep-rolling-clj.core
  (:require [keep-rolling-clj.utils :as utils])
  (:import [java.util.concurrent Executors ThreadPoolExecutor ExecutorService TimeUnit]))


(def default-step-params
  {:on-failure         :bail
   :delay              5
   :retries            ##Inf
   :parallel-execution false
   :only-once          false})


(def default-global-params
  {:host-filter        ".*"
   :step-parallelism   10
   :recipe-parallelism 1})


(defn gen-entity-map [entities-coll]
  (reduce (fn [acc val] (assoc-in acc [(:type val) (:name val)] val)) {} entities-coll))


(def entity-map (gen-entity-map (utils/get-all-plugins)))


(def step-already-failed-error-code 126)


(defn extract-params [entity params]
  (select-keys params (:required-params entity)))


(defn throw-exception-if [predicate ^String msg & coll]
  (if (apply predicate coll)
    (throw (Exception. msg))
    (last coll)))


(defn extract-and-check-params [entity params]
  (let [extracted-params (extract-params entity params)
        required-params  (:required-params entity)]
    (throw-exception-if (comp not utils/equal-count?) (str "Invalid parameters passed to " (:name entity) ": " params ", wanted: " required-params) required-params extracted-params)
    extracted-params))


(defn make-step-exec-f [step params]
  (fn [] ((:handler step) params)))


(defn classify [classifier params]
  (let [classifier-res ((:handler classifier) params)]
    (throw-exception-if empty? (str "Classifier " classifier " did not return anything when called with " params) classifier-res)))


(defn run-in-parallel [function params ^ThreadPoolExecutor pool]
  (let [tasks   (map (fn [p] #(apply function p)) params)
        results (map #(.get %) (.invokeAll pool tasks))]
    results))


(def immediate-return (ref false))
(def only-once-executed (ref {}))


(defn step-failed-msg []
  {:err step-already-failed-error-code :err-msg "Another step already failed"})


(defn make-execution-description-string [step host]
  (str (utils/pad-step (str (:name step)))
       (utils/pad-service (if (:service step) (str "service " (str (:service step)) "")))
       " @ " host))


(defn run-step [step params]
  (let [{:keys [retries delay]} step
        extracted-params             (extract-and-check-params step params)
        execution-description-string (make-execution-description-string step (:host params))
        _                            (utils/safe-println (utils/colorize :green (str (utils/pad-stage "Run:") execution-description-string " with " extracted-params)))
        exec-f                       (make-step-exec-f step extracted-params)
        step-ret                     (reduce
                                       (fn [_ f]
                                         (if @immediate-return
                                           (reduced (step-failed-msg))
                                           (let [step-ret (f)]
                                             (if (utils/no-err-ret? step-ret)
                                               (reduced step-ret)
                                               (do (utils/safe-println (utils/colorize
                                                                         :yellow
                                                                         (utils/make-code-and-msg-string (str (utils/pad-stage "Retry:") execution-description-string ", last error was: ") step-ret)))
                                                   (Thread/sleep (* delay 1000))
                                                   step-ret)))))
                                       utils/no-err-ret
                                       (if (= ##Inf retries)
                                         (repeat exec-f)
                                         (repeat retries exec-f)))
        strategy                     (:on-failure step)]
    (if (utils/no-err-ret? step-ret)
      step-ret
      (case strategy
        (:bail) (do (utils/safe-println (utils/colorize
                                          :red
                                          (utils/make-code-and-msg-string (str (utils/pad-stage "Abort:") execution-description-string ", cause: ") step-ret)))
                    (dosync
                      (ref-set immediate-return true))
                    step-ret)

        (:skip) (do (utils/safe-println
                      (utils/colorize
                        :blue
                        (utils/make-code-and-msg-string (str (utils/pad-stage "Skip:") execution-description-string ", cause: ") step-ret)))
                    utils/no-err-ret)))))


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


(defn run-step-group-on-host [steps host params]
  (let [params-with-host (assoc params :host host)]
    (reduce (fn [_ step]
              (let [parallel     (:parallel-execution step)
                    execute?     (if-not (:only-once step)
                                   true
                                   (dosync
                                     (let [was-executed? (get @only-once-executed step)]
                                       (if was-executed?
                                         false
                                         (do
                                           (alter only-once-executed assoc step true)
                                           true)))))

                    run-step-ret (if execute?
                                   (if parallel
                                     (-> (run-in-parallel run-step (map (fn [host] [step (assoc params :host host)]) (:hosts params)) (:step-pool params))
                                         (utils/remove-no-err-ret)
                                         (first))
                                     (run-step step params-with-host))
                                   utils/no-err-ret)]
                (when-not (utils/no-err-ret? run-step-ret)
                  ;(println-code-and-msg "Last error: " run-step-ret)
                  (reduced run-step-ret))
                run-step-ret))
            utils/no-err-ret
            steps)))


(defn run-step-group [steps params]
  (run-in-parallel run-step-group-on-host (map (fn [host] [steps host params]) (:hosts params)) (:recipe-pool params)))


(defn run-steps [step-groups params]
  (loop [s        step-groups
         hosts    (:hosts params)
         prev-ret utils/no-err-ret]
    (if (or (empty? hosts) (empty? s) (utils/err-ret? prev-ret))
      prev-ret
      (let [[f-s & r-s] s]
        (cond
          (vector? f-s) (recur r-s hosts (-> (run-step-group f-s params) (utils/remove-no-err-ret) (first)))
          :default (run-step-group s params))))))


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
        (assoc :handler (get service action-type (fn [& _] utils/no-err-ret)))
        (assoc :service (:name service))
        (assoc :required-params (:required-params service)))))


(defn expand-service-step [step services]
  (let [action-type (:action-type step)]
    (if (= action-type :handler)
      [step]
      (map (partial service-to-step step) services))))


(defn check-steps-nesting [steps]
  (throw-exception-if
    (comp not utils/deep-same-type)
    (str "Steps collection must not have dangling steps: " steps)
    steps))


(defn expand-params [params]
  ;; params -> params-with-defaults -> params-recipe-enriched -> params-with-pools -> params-with-hosts -> expanded-params

  (let [{classifier-name :classifier recipe-name :recipe} params
        params-with-defaults   (merge default-global-params params)
        host-filter            (:host-filter params-with-defaults)
        classifier             (get-classifier classifier-name)
        recipe                 (get-recipe recipe-name)
        params-recipe-addition ((get recipe :handler (fn [& _] {})) params-with-defaults)
        params-recipe-enriched (merge params-with-defaults params-recipe-addition)
        params-with-pools      (-> params-recipe-enriched
                                   (assoc :step-pool (Executors/newFixedThreadPool (:step-parallelism params-with-defaults)))
                                   (assoc :recipe-pool (Executors/newFixedThreadPool (:recipe-parallelism params-with-defaults))))
        hosts                  (run-classifier classifier params-recipe-enriched)
        filtered-hosts         (filter #(re-find (re-pattern host-filter) %) hosts)
        params-with-hosts      (-> params-with-pools
                                   (assoc :hosts filtered-hosts)
                                   (assoc :all-hosts hosts))

        matching-services      (get-matching-services (services-vec) params-with-hosts)

        steps                  (->> (:steps recipe)
                                    (check-steps-nesting)
                                    (utils/deep-map-scalar get-step)
                                    (utils/deep-map-scalar #(merge default-step-params %)))

        expanded-steps         (utils/deep-map-coll #(expand-service-step % matching-services) steps)
        expanded-params        (assoc params-with-hosts :steps expanded-steps)]

    expanded-params))


(defn shutdown-pool [^ExecutorService pool timeout]
  (.shutdown pool)
  (try
    (when-not (.awaitTermination pool timeout TimeUnit/SECONDS)
      (.shutdownNow pool)
      (if (not (.awaitTermination pool timeout TimeUnit/SECONDS))
        (utils/safe-println "Pool did not terminate")))
    (catch InterruptedException ie
      (.shutdownNow pool)
      (.interrupt (Thread/currentThread)))))


(defn shutdown-pools [params]
  (doseq [pool [(:recipe-pool params) (:step-pool params)]]
    (shutdown-pool pool 10))
  (flush))


(defn run [params]
  (dosync (ref-set immediate-return false))
  (let [expanded-params (expand-params params)
        ret             (run-steps (:steps expanded-params) expanded-params)]
    (shutdown-pools expanded-params)
    (if (and (utils/no-err-ret? ret) (not @immediate-return))
      (utils/safe-println "Finished successfully")
      (utils/safe-println (utils/colorize :red "Failed")))))



(run {:classifier :test-classifier1
      :cluster    "kafka"
      :recipe     :test-recipe1
      :message    "hui"})
