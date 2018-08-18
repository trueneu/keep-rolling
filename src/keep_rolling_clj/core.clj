(ns keep-rolling-clj.core
  (:require [keep-rolling-clj.protocols :refer :all]
            keep-rolling-clj.utils))

(def plugins-path "/Users/pgurkov/git_tree/keep-rolling-clj/src/keep_rolling_clj/plugins")

(defn load-plugins []
  (let [all-files (file-seq (clojure.java.io/file plugins-path))
        without-dirs (remove #(.isDirectory %) all-files)
        only-clj (filter #(.endsWith (.getName %) ".clj") without-dirs)]
    (doseq [plugin only-clj]
      (load-file (.getPath plugin)))))

(defonce _ (load-plugins))  ; hackity hacks

(defn make-plugins-map []
  (let [all-plugins (mapcat ns-publics (filter #(= (:kr-type (meta %)) :ns) (all-ns)))
        plugins-by-name (apply hash-map (flatten all-plugins))
        plugins-by-type (reduce (fn [acc [p-symbol p-var]]
                                  (let [key (-> p-var meta :kr-type)
                                        value p-var]
                                    (assoc acc key (conj (acc key) value)))) {} all-plugins)]
    (list plugins-by-name plugins-by-type)))

;(defn get-extenders [protocol]
;  (let [all-imports (apply merge (map ns-imports (all-ns)))
;        classes (filter (fn [cl] (extends? protocol cl))
;                        (vals all-imports))]
;    classes))
;
;(defn make-plugins-map []
;  (let [protocols [IStep IRecipe]]
;    (apply hash-map (interleave protocols (map get-extenders protocols)))))

;(defn main [args]
;  (load-plugins)  ;; excuse me, but we'll deal with state a little bit here
;  (let [plugins-map (make-plugins-map)]))

(defn run-test-recipe []
  (let [recipe-name "recipe1"
        [plugins-map by-type] (make-plugins-map)
        recipe-f (keep-rolling-clj.utils/plugin-by-name plugins-map recipe-name)]
    (println plugins-map)
    (println recipe-f)
    (recipe-f {:plugins-map plugins-map})))
