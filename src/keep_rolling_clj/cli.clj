(ns keep-rolling-clj.cli
  (:require [keep-rolling-clj.plugins :as plugins]
            [keep-rolling-clj.core :as core]
            [clojure.tools.cli :as cli]
            [clojure.string :as string]
            [clojure.walk :as walk]))

(def cli-options
  [["-k" "--classifier classifier" "Classifier name, required"
    :parse-fn keyword]
   ["-r" "--recipe recipe" "Recipe name, required"
    :parse-fn keyword]
   ["-c" "--cluster cluster" "Cluster name, required"]
   ["-f" "--host-filter host-filter" "Host filter regex"
    :default ".*"]
   ["-p" "--param key=value" "Arbitrary parameters (see plugins parameters below)"
    :assoc-fn (fn [m k v]
                (let [[prop val] (string/split v #"=")]
                  (update m k assoc prop val)))]
   [nil "--recipe-threads num-threads" "Number of threads for recipe-level parallelism. Specify >1 to run recipe in parallel"
    :default 1
    :parse-fn #(Integer/parseInt %)]
   [nil "--step-threads num-threads" "Number of threads for step-level parallelism"
    :default 10
    :parse-fn #(Integer/parseInt %)]
   ["-v" nil "Verbosity level; may be specified multiple times"
    :id :verbosity
    :default 0
    :update-fn inc]
   ["-h" "--help"]])


(defn usage [options-summary plugins-params-summary]
  (->> ["This is my program. There are many like it, but this one is mine."
        ""
        "Usage: herebename [options]"  ;; fixme
        ""
        "Options:"
        options-summary
        ""
        "Plugins parameters summary:"
        plugins-params-summary
        ""]
       (string/join \newline)))


(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (string/join \newline errors)))


(comment
  (def plugins-params (get-plugins-params))
  plugins-params
  (apply println (plugins-params-msg plugins-params))
  (println (:exit-message (validate-args ["-h"])))
  (validate-args (string/split "-r test-recipe1 -c kafka -k test-classifier1 -p message=hi" #"\s")))


(def required-args [:classifier :recipe :cluster])


(defn validate-args
  [args]
  (let [{:keys [options errors summary]} (cli/parse-opts args cli-options)
        plugins-params-summary (plugins/plugins-params-and-docs-msg (plugins/get-plugins-params-and-docs))
        usage-msg (usage summary plugins-params-summary)]
    (cond
      (:help options) {:exit-message usage-msg :ok? true}
      errors          {:exit-message (string/join \newline [(error-msg errors) "" usage-msg])}
      (and
        (not-any? nil? (map #(get options %) required-args))
        (or
          (nil? (:param options))
          (not-any? nil? (vals (:param options)))))
      {:options options}
      :else           {:exit-message usage-msg})))

(defn exit [status msg]
  (println msg))
  ;(System/exit status))

(defn -main [& args]
  (let [{:keys [options exit-message ok?]} (validate-args args)]
    (if exit-message
      (exit (if ok? 0 1) exit-message)
      (let [param (:param options)
            kr-params (-> options
                          (merge (walk/keywordize-keys param))
                          (dissoc :param))]
        ;kr-params))))
        (core/run kr-params)))))


(comment
  (apply -main (string/split "-r test-recipe1 -c kafka -k test-classifier1 -p message=hi" #"\s")))