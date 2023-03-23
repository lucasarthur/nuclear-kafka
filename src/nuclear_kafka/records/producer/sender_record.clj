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

(ns nuclear-kafka.records.producer.sender-record
  (:refer-clojure :exclude [key partition])
  (:require
   [nuclear-kafka.records.headers :refer [->record-header-seq]])
  (:import
   (org.apache.kafka.clients.producer ProducerRecord)
   (reactor.kafka.sender SenderRecord)))

(defn ->sender-record
  [{:keys [topic key value headers partition timestamp correlation-metadata]}]
  (-> (ProducerRecord. (name topic) partition timestamp key value (->record-header-seq headers))
      (SenderRecord/create correlation-metadata)))

(defn topic [record]
  (-> record .topic keyword))

(defn headers [record]
  (into [] (.headers record)))

(defn key [record]
  (.key record))

(defn value [record]
  (.value record))

(defn timestamp [record]
  (.timestamp record))

(defn partition [record]
  (.partition record))

(defn correlation-metadata [record]
  (.correlationMetadata record))

(defn sender-record->map [record]
  (->> {:topic (topic record)
        :key (key record)
        :value (value record)
        :timestamp (timestamp record)
        :partition (partition record)
        :correlation-metadata (correlation-metadata record)}
       (filter (comp some? val))
       (into {})))
