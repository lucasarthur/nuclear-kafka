(ns nuclear-kafka.records.partition-info
  (:refer-clojure :exclude [partition])
  (:require
   [nuclear-kafka.records.node :refer [node->map]]))

(defn topic [info]
  (-> info topic keyword))

(defn partition [info]
  (.partition info))

(defn leader [info]
  (-> info .leader node->map))

(defn replicas [info]
  (->> info .replicas (into []) (map node->map)))

(defn in-sync-replicas [info]
  (->> info .inSyncReplicas (into []) (map node->map)))

(defn offline-replicas [info]
  (->> info .offlineReplicas (into []) (map node->map)))

(defn partition-info->map [info]
  (->> {:topic (topic info)
        :partition (partition info)
        :leader (leader info)
        :replicas (replicas info)
        :in-sync-replicas (in-sync-replicas info)
        :offline-replicas (offline-replicas info)}
       (filter (comp some? val))
       (into {})))
