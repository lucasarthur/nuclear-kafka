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

(ns nuclear-kafka.records.consumer.receiver-offset
  (:require
   [nuclear-kafka.records.topic-partition :refer [topic-partition->map]]))

(defn topic-partition [receiver-offset]
  (-> receiver-offset .topicPartition topic-partition->map))

(defn offset [receiver-offset]
  (.offset receiver-offset))

(defn ack [receiver-offset]
  (.acknowledge receiver-offset)
  receiver-offset)

(defn commit [receiver-offset]
  (.commit receiver-offset))

(defn receiver-offset->map [receiver-offset]
  {:topic-partition (topic-partition receiver-offset)
   :offset (offset receiver-offset)
   :ack #(ack receiver-offset)
   :commit #(commit receiver-offset)})
