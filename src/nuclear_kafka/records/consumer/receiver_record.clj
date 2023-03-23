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

(ns nuclear-kafka.records.consumer.receiver-record
  (:refer-clojure :exclude [partition key])
  (:require
   [nuclear-kafka.records.headers :refer [headers->map]]
   [nuclear-kafka.records.timestamp-type :refer [timestamp-types]]
   [nuclear-kafka.records.consumer.receiver-offset :refer [receiver-offset->map]]))

(defn receiver-offset [record]
  (-> record .receiverOffset receiver-offset->map))

(defn topic [record]
  (-> record .topic keyword))

(defn partition [record]
  (.partition record))

(defn headers [record]
  (-> record .headers headers->map))

(defn key [record]
  (.key record))

(defn value [record]
  (.value record))

(defn offset [record]
  (.offset record))

(defn timestamp [record]
  (.timestamp record))

(defn timestamp-type [record]
  (-> record .timestampType (get timestamp-types)))

(defn serialized-key-size [record]
  (.serializedKeySize record))

(defn serialized-value-size [record]
  (.serializedValueSize record))

(defn leader-epoch [record]
  (-> record .leaderEpoch (.orElse -1)))

(defn receiver-record->map [record]
  (->> {:topic (topic record)
        :key (key record)
        :value (value record)
        :headers (headers record)
        :partition (partition record)
        :offset (offset record)
        :receiver-offset (receiver-offset record)
        :timestamp (timestamp record)
        :timestamp-type (timestamp-type record)
        :serialized-key-size (serialized-key-size record)
        :serialized-value-size (serialized-value-size record)
        :leader-epoch (leader-epoch record)}))
