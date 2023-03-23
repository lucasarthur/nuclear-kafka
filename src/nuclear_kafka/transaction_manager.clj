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

(ns nuclear-kafka.transaction-manager
  (:require
   [nuclear-kafka.records.topic-partition :refer [->topic-partition]]
   [nuclear.util.sam :refer [->consumer ->function]])
  (:import
   (org.apache.kafka.clients.consumer OffsetAndMetadata ConsumerGroupMetadata)
   (java.util Optional)))

(defn ->offset-and-metadata
  [{:keys [offset leader-epoch metadata]}]
  (let [le-opt (-> (Optional/ofNullable leader-epoch)
                   (.map (->function #(.intValue %))))]
    (OffsetAndMetadata. offset le-opt metadata)))

(defn ->consumer-group-metadata
  [{:keys [group-id gen-id member-id group-instance-id]}]
  (ConsumerGroupMetadata. group-id gen-id member-id (Optional/ofNullable group-instance-id)))

(defn begin-tx [txm]
  (.begin txm))

(defn commit-tx [txm]
  (.commit txm))

(defn abort-tx [txm]
  (.abort txm))

(defn tx-scheduler [txm]
  (.scheduler txm))

(defn on-tx-complete! [consumer txm]
  (->> consumer ->consumer (.transactionComplete txm)))

(defn send-offsets [tpms cgm txm]
  (.sendOffsets
   txm
   (->> (partition 3 tpms)
        (map (fn [tpm]
               [(->topic-partition (first tpm) (second tpm))
                (-> tpm last ->offset-and-metadata)]))
        (into {}))
   (->consumer-group-metadata cgm)))
