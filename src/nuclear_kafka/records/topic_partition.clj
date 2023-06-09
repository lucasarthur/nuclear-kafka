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

(ns nuclear-kafka.records.topic-partition
  (:refer-clojure :exclude [partition])
  (:import
   (org.apache.kafka.common TopicPartition)))

(defn topic [tp]
  (-> tp .topic keyword))

(defn partition [tp]
  (.partition tp))

(defn ->topic-partition
  ([[t p]] (->topic-partition t p))
  ([topic partition] (TopicPartition. (name topic) partition)))

(defn topic-partition->map [tp]
  {:topic (topic tp)
   :partition (partition tp)})
