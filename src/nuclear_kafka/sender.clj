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

(ns nuclear-kafka.sender
  (:require
   [nuclear-kafka.config.producer :refer [->producer-options]]
   [nuclear-kafka.records.shape :refer [producer-shape]]
   [nuclear-kafka.records.producer.result :refer [result->map]]
   [nuclear.core :as nk]
   [nuclear.mono :as mono]
   [nuclear.util.sam :refer [->function]])
  (:import
   (reactor.kafka.sender KafkaSender)))

(defn close! [{:keys [sender]}]
  (.close sender))

(defn ->sender [sender-options]
  (let [sender (-> sender-options ->producer-options KafkaSender/create)]
    {:sender sender
     :close! #(close! {:sender sender})
     :shape (:shape sender-options)}))

(defn send-many
  [records {:keys [sender shape]}]
  (->> (nk/map #(producer-shape shape %) records) (.send sender) (nk/map result->map)))

(defn send-one [record sender]
  (-> (mono/->flux record) (send-many sender) (nk/single)))

(defn tx-send
  [tx-records {:keys [sender shape]}]
  (->> (nk/map (fn [windows] (nk/map #(producer-shape shape %) windows)) tx-records)
       (.sendTransactionally sender)
       (nk/map (fn [results] (nk/map result->map results)))))

(defn tx-manager [{:keys [sender]}]
  (.transactionManager sender))

(defn on-producer! [producer-fn {:keys [receiver]}]
  (->> producer-fn ->function (.doOnProducer receiver)))
