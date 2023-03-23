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

(ns nuclear-kafka.records.producer.result
  (:require
   [nuclear-kafka.util :refer [update-if-contains]]
   [nuclear-kafka.records.producer.record-metadata :refer [record-metadata->map]]))

(defn exception [result]
  (.exception result))

(defn correlation-metadata [result]
  (.correlationMetadata result))

(defn record-metadata [result]
  (.recordMetadata result))

(defn result->map [result]
  (->> {:error (exception result)
        :correlation-metadata (correlation-metadata result)
        :record-metadata (record-metadata result)}
       (filter (comp some? val))
       (into {})
       (update-if-contains :record-metadata record-metadata->map)))
