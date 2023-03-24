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

(ns nuclear-kafka.receiver
  (:require
   [nuclear-kafka.config.consumer :refer [->consumer-options]]
   [nuclear-kafka.records.consumer.receiver-record :refer [receiver-record->map]]
   [nuclear-kafka.records.shape :refer [consumer-shape]]
   [nuclear.core :as nk]
   [nuclear.util.sam :refer [->function]])
  (:import
   (reactor.kafka.receiver KafkaReceiver)))

(defn ->receiver [receiver-options]
  {:receiver (-> receiver-options ->consumer-options KafkaReceiver/create)
   :shape (:shape receiver-options)})

(defn receive
  ([receiver]
   (receive nil receiver))
  ([prefetch {:keys [receiver shape]}]
   (->> (.receive receiver prefetch)
        (nk/map receiver-record->map)
        (nk/map #(consumer-shape shape %)))))

(defn receive-auto-ack
  ([receiver]
   (receive-auto-ack nil receiver))
  ([prefetch {:keys [receiver shape]}]
   (->> (.receiveAutoAck receiver prefetch)
        (nk/flat-map identity)
        (nk/map receiver-record->map)
        (nk/map #(consumer-shape shape %)))))

(defn receive-atmost-once
  ([receiver]
   (receive-atmost-once nil receiver))
  ([prefetch {:keys [receiver shape]}]
   (->> (.receiveAtmostOnce receiver prefetch)
        (nk/map receiver-record->map)
        (nk/map #(consumer-shape shape %)))))

(defn receive-exactly-once
  ([txm receiver]
   (receive-exactly-once txm nil receiver))
  ([txm prefetch {:keys [receiver shape]}]
   (->> (.receiveExactlyOnce receiver txm prefetch)
        (nk/map
         (fn [records]
           (->> (nk/map receiver-record->map records)
                (nk/map #(consumer-shape shape %))))))))

(defn on-consumer! [consumer-fn {:keys [receiver]}]
  (->> consumer-fn ->function (.doOnConsumer receiver)))
