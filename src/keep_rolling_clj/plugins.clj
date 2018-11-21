(ns keep-rolling-clj.plugins
  (:require [keep-rolling-clj.utils :as utils]
            [clojure.string :as string])
  (:import (java.io File)))


(def plugins-path "/Users/pgurkov/git_tree/keep-rolling-clj/src/keep_rolling_clj/plugins")
(def internal-params #{:host :all-hosts :cluster :step-threads :recipe-threads :recipe :host-filter :verbosity})


(defn load-plugins []
  (let [all-files (file-seq (File. ^String plugins-path))
        without-dirs (remove #(.isDirectory ^File %) all-files)
        only-clj (filter #(.endsWith (.getName %) ".clj") without-dirs)]
    (doseq [plugin only-clj]
      (load-file (.getPath plugin)))))


(load-plugins)  ;; purely for side effects :)


(defn get-all-plugins []
  (->> (all-ns)
   (mapcat ns-publics)
   (map second)
   (filter #(-> % meta :kr))
   (map deref)))


(defn gen-entity-map [entities-coll]
  (reduce (fn [acc val] (assoc-in acc [(:type val) (:name val)] val)) {} entities-coll))


(def entity-map (gen-entity-map (get-all-plugins)))


(defn exists? [entity-type entity-name]
  (contains? (entity-type entity-map) entity-name))

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


(defn get-plugins-params-and-docs []
  (->>
    (get-all-plugins)
    (map (juxt :name
               #(remove internal-params (:required-params %))
               :doc))
    (sort-by first)))


(defn plugins-params-and-docs-msg [plugins-params]
  (string/join
    (reduce (fn [acc val]
              (let [[entity-name entity-params entity-doc] val]
                (if (empty? entity-params)
                  acc
                  (-> acc
                      (conj (name entity-name) ":\n  ")
                      (conj "Params: " (string/join ", " (map name entity-params)))
                      (conj "\n  " entity-doc "\n")))))
            []
            plugins-params)))
