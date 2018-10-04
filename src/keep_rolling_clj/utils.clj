(ns keep-rolling-clj.utils)

(def plugins-path "/Users/pgurkov/git_tree/keep-rolling-clj/src/keep_rolling_clj/plugins")

(defn load-plugins []
  (let [all-files (file-seq (clojure.java.io/file plugins-path))
        without-dirs (remove #(.isDirectory %) all-files)
        only-clj (filter #(.endsWith (.getName %) ".clj") without-dirs)]
    (doseq [plugin only-clj]
      (load-file (.getPath plugin)))))

(defonce _ (load-plugins))  ; hackity hacks


