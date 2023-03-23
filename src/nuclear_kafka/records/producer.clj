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

(ns nuclear-kafka.records.producer
  (:require
   [nuclear-kafka.transaction-manager :refer [->consumer-group-metadata ->offset-and-metadata]]
   [nuclear-kafka.records.topic-partition :refer [->topic-partition]]
   [nuclear-kafka.records.partition-info :refer [partition-info->map]]
   [nuclear-kafka.records.metric :refer [metric->map metric-name->map]]))

(defn partitions-for [topic producer]
  (->> (name topic)
       (.partitionsFor producer)
       (into [])
       (map partition-info->map)))

(defn metrics [producer]
  (->> (.metrics producer)
       (into {})
       (map (fn [[k v]] [(metric-name->map k) (metric->map v)]))))

(defn send-offsets-to-tx [tpms cgm producer]
  (.sendOffsetsToTransaction
   producer
   (->> (partition 3 tpms)
        (map (fn [tpm]
               [(->topic-partition (first tpm) (second tpm))
                (-> tpm last ->offset-and-metadata)]))
        (into {}))
   (->consumer-group-metadata cgm))
  producer)

(defn flush! [producer]
  (.flush producer)
  producer)
