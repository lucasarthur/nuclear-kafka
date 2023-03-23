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

(ns nuclear-kafka.records.producer.record-metadata
  (:refer-clojure :exclude [partition]))

(defn has-offset? [metadata]
  (.hasOffset metadata))

(defn has-timestamp? [metadata]
  (.hasTimestamp metadata))

(defn offset [metadata]
  (.offset metadata))

(defn timestamp [metadata]
  (.timestamp metadata))

(defn serialized-key-size [metadata]
  (.serializedKeySize metadata))

(defn serialized-value-size [metadata]
  (.serializedValueSize metadata))

(defn topic [metadata]
  (.topic metadata))

(defn partition [metadata]
  (.partition metadata))

(defn record-metadata->map [metadata]
  {:serialized-key-size (serialized-key-size metadata)
   :serialized-value-size (serialized-value-size metadata)
   :topic (-> metadata topic keyword)
   :partition (partition metadata)
   :offset (offset metadata)
   :timestamp (timestamp metadata)})
