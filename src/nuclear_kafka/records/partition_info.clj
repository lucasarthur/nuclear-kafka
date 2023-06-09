;; Copyright (c) 2023 Lucas Arthur
;;
;; This file is part of Nuclear Kafka, an open-source library whose goal is to
;; use Reactor Kafka with Clojure in an idiomatic and simplified way.
;;
;; Nuclear Kafka is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.
;;
;; Nuclear Kafka is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
;; GNU General Public License for more details.
;;
;; You should have received a copy of the GNU General Public License
;; along with Nuclear Kafka. If not, see <http://www.gnu.org/licenses/>.

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
