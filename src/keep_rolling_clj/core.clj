(ns keep-rolling-clj.core
  (:require [keep-rolling-clj.utils :as utils])
  (:import [Double])
  (:import [java.util.concurrent Executors ThreadPoolExecutor ExecutorService TimeUnit]))


(def default-step-params
  {:on-failure         :bail
   :delay              5
   :retries            Double/POSITIVE_INFINITY
   :parallel-execution false})


(def default-global-params
  {:host-filter        ".*"
   :step-parallelism   10
   :recipe-parallelism 1})                                  ;; TODO change this to 1


(defn gen-entity-map [entities-coll]
  (reduce (fn [acc val] (assoc-in acc [(:type val) (:name val)] val)) {} entities-coll))


(def entity-map (gen-entity-map (utils/get-all-plugins)))
(def no-err-ret {:err nil :err-msg nil})


(defn remove-no-err-ret [coll]
  (remove #(= % no-err-ret) coll))

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


(defn loop-if [predicate delay retries retry-f f]  ;; eto EBOK
  (loop [ret   (f)
         count 0]
    (cond
      (not (predicate ret)) ret
      (>= count retries) ret
      :default (do (retry-f ret)
                   (Thread/sleep (* 1000 delay))
                   (recur (f) (inc count))))))


(defn bail-or-skip [step step-ret]
  (let [t (get step :on-failure :bail)]
    (if (utils/no-err-ret? step-ret)
      step-ret
      (cond
        (= t :skip) (do (utils/safe-println (str "Step " (:name step) " failed. Skipping...")) no-err-ret)
        (= t :bail) (do (utils/safe-println (str "Step " (:name step) " failed. Aborting...")) step-ret)))))


(defn make-step-exec-f [step params]
  (fn [] ((:handler step) params)))


(defn classify [classifier params]
  (let [classifier-res ((:handler classifier) params)]
    (throw-exception-if empty? (str "Classifier " classifier " did not return anything when called with " params) classifier-res)))


(defn run-in-parallel [function params ^ThreadPoolExecutor pool]
  (let [tasks   (map (fn [p] #(apply function p)) params)
        results (map #(.get %) (.invokeAll pool tasks))]
    ;(.shutdown pool)
    ;(utils/locked-println results)
    results))


(def immediate-return (ref false))


(defn step-failed-msg []
  {:err 111 :err-msg "Another step already failed"})  ;; 111 is a good number, sure, but...


(defn run-step [step params]
  (let [{:keys [retries delay]} step
        extracted-params (extract-and-check-params step params)
        _                (utils/safe-print-debug 1 (str "Run: " (:name step) (if (:service step) (str ", service " (:service step)) "") " on host " (:host params) " with " extracted-params)) ; fixme form msg as a fn
        exec-f           (make-step-exec-f step extracted-params)
        step-ret         (reduce
                           (fn [_ f]
                             (if @immediate-return
                               (reduced (step-failed-msg))
                               (let [step-ret (f)]
                                 (if (no-err-ret step-ret)
                                   (reduced step-ret)
                                   (do (utils/safe-println-code-and-msg (str "Retry: " (:name step) (if (:service step) (str ", service " (:service step)) "") " on host " (:host params) ", last error was: ") step-ret)
                                       (Thread/sleep (* delay 1000))
                                       step-ret)))))
                           no-err-ret
                           (repeat retries exec-f))
        strategy (:on-failure step)]
    (if (utils/no-err-ret? step-ret)
      step-ret
      (case strategy
        (:bail) (do (utils/safe-println-code-and-msg (str "Abort: " (:name step) (if (:service step) (str ", service " (:service step)) "") " on host " (:host params) ", cause: ") step-ret)
                    (dosync
                      (ref-set immediate-return true))
                    step-ret)

        (:skip) (do (utils/safe-println (str "Skip: " (:name step) (if (:service step) (str ", service " (:service step)) "") " on host " (:host params)))
                    no-err-ret)))))





  ;(->> params
  ;     (extract-and-check-params step)
  ;     (make-step-exec-f step)
  ;     (bail-or-skip step)))


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
                    run-step-ret (if parallel
                                   ;; todo test when steps fail!  -- doesn't work
                                   (-> (run-in-parallel run-step (map (fn [host] [step (assoc params :host host)]) (:hosts params)) (:step-pool params))
                                       (remove-no-err-ret)
                                       (first))
                                   (run-step step params-with-host))]
                (when-not (utils/no-err-ret? run-step-ret)
                  ;(println-code-and-msg "Last error: " run-step-ret)
                  (reduced run-step-ret))
                run-step-ret))
            no-err-ret
            steps)))


(defn run-step-group-on-hosts [steps params]
  (reduce (fn [_ host]
            (let [run-step-group-ret (run-step-group-on-host steps host params)]
              (when-not (utils/no-err-ret? run-step-group-ret)
                (reduced run-step-group-ret))))
          no-err-ret
          (:hosts params)))


(defn run-step-group [steps params]
  (run-in-parallel run-step-group-on-host (map (fn [host] [steps host params]) (:hosts params)) (:recipe-pool params)))


(defn run-steps [step-groups params]
  (loop [s        step-groups
         hosts    (:hosts params)
         prev-ret no-err-ret]
    (if (or (empty? hosts) (empty? s) (utils/err-ret? prev-ret))
      prev-ret
      (let [[f-s & r-s] s]
        (cond
          (vector? f-s) (recur r-s hosts (-> (run-step-group f-s params) (remove-no-err-ret) (first)))
          :default (run-step-group s params))))))
          ;(vector? f-s) (recur r-s hosts (run-step-group-on-hosts f-s params))
          ;:default (run-step-group-on-hosts s params))))))


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
        (assoc :handler (get service action-type (fn [& _] no-err-ret)))
        (assoc :service (:name service)))))


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
        params-recipe-addition ((:handler recipe) params-with-defaults)
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
  ;(doseq [pool (doall (map #(get params %) [:step-pool :recipe-pool]))])
  (doseq [pool [(:recipe-pool params) (:step-pool params)]]
    (shutdown-pool pool 10))
  (flush))

  ;(utils/locked-println "terminated? " (.isTerminated (:recipe-pool params)))
  ;(utils/locked-println "terminated? " (.isTerminated (:step-pool params)))
  ;(utils/locked-println "shut? " (.isShutdown (:recipe-pool params)))
  ;(utils/locked-println "shut? " (.isShutdown (:step-pool params))))


(defn run [params]
  (let [expanded-params (expand-params params)
        _ (utils/safe-print-debug 1 expanded-params)
        ret (run-steps (:steps expanded-params) expanded-params)]
    (shutdown-pools expanded-params)
    (if (utils/no-err-ret? ret)
      (utils/safe-println "Finished successfully")
      (utils/safe-println-code-and-msg "Failed, last error: " ret))))


(run {:classifier :test-classifier1
      :cluster    "kafka"
      :recipe     :test-recipe1
      :message    "hui"})
