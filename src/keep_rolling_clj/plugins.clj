(ns keep-rolling-clj.plugins
  (:require [keep-rolling-clj.utils :as utils])
  (:import (java.io File)))


(def plugins-path "/Users/pgurkov/git_tree/keep-rolling-clj/src/keep_rolling_clj/plugins")


(defn load-plugins []
  (let [all-files (file-seq (File. ^String plugins-path))
        without-dirs (remove #(.isDirectory ^File %) all-files)
        only-clj (filter #(.endsWith (.getName %) ".clj") without-dirs)]
    (doseq [plugin only-clj]
      (load-file (.getPath plugin)))))


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

