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

(ns nuclear-kafka.records.consumer
  (:require
   [nuclear-kafka.records.topic-partition :refer [->topic-partition topic-partition->map]]
   [nuclear-kafka.records.partition-info :refer [partition-info->map]]
   [nuclear-kafka.records.metric :refer [metric->map metric-name->map]]))

(defn assignment [consumer]
  (->> consumer .assignment (into #{}) (map topic-partition->map)))

(defn subscription [consumer]
  (->> consumer .subscription (into #{})))

(defn seek [[t p] offset consumer]
  (.seek consumer (->topic-partition t p) offset))

(defn seek-to-beginning [tps consumer]
  (->> tps (partition 2) (map ->topic-partition) (.seekToBeginning consumer)))

(defn seek-to-end [tps consumer]
  (->> tps (partition 2) (map ->topic-partition) (.seekToEnd consumer)))

(defn position [[t p] consumer]
  (->> (->topic-partition t p) (.position consumer)))

(defn commited [[t p] consumer]
  (let [offset-and-meta (.commited consumer (->topic-partition t p))]
    {:offset (.offset offset-and-meta)
     :metadata (.metadata offset-and-meta)
     :leader-epoch (-> offset-and-meta .leaderEpoch (.orElse -1))}))

(defn metrics [consumer]
  (->> (.metrics consumer)
       (into {})
       (map (fn [[k v]] [(metric-name->map k) (metric->map v)]))))

(defn partitions-for [topic consumer]
  (->> (name topic)
       (.partitionsFor consumer)
       (into [])
       (map partition-info->map)))

(defn list-topics [consumer]
  (->> (.listTopics consumer)
       (into {})
       (map (fn [[k v]] [k (->> v (into []) (map partition-info->map))]))))

(defn paused [consumer]
  (->> consumer .paused (into #{}) (map topic-partition->map)))

(defn pause [tps consumer]
  (->> tps (partition 2) (map ->topic-partition) (.pause consumer)))

(defn resume [tps consumer]
  (->> tps (partition 2) (map ->topic-partition) (.resume consumer)))

(defn offsets-for-times [tpts consumer]
  (->> (partition 3 tpts)
       (reduce #(assoc %1 (->topic-partition (first %2) (second %2)) (last %2)) {})
       (.offsetForTimes consumer)
       (into {})
       (map (fn [[k v]]
              [(topic-partition->map k)
               {:timestamp (.timestamp v)
                :offset (.offset v)
                :leader-epoch (-> v .leaderEpoch (.orElse -1))}]))))

(defn beginning-offsets [tps consumer]
  (->> (partition 2 tps)
       (map ->topic-partition)
       (.beginningOffsets consumer)
       (into {})
       (map (fn [[k v]] [(topic-partition->map k) v]))))

(defn end-offsets [tps consumer]
  (->> (partition 2 tps)
       (map ->topic-partition)
       (.endOffsets consumer)
       (into {})
       (map (fn [[k v]] [(topic-partition->map k) v]))))
