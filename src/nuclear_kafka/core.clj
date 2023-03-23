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

(ns nuclear-kafka.core
  (:require
   [nuclear.core :as nk]
   [nuclear-kafka.deserializer :refer [keyword-deserializer json-deserializer]]
   [nuclear-kafka.receiver :refer [->receiver receive-auto-ack]]))

(def receiver-cfg
  {:brokers ["localhost" 9092]
   :topics [:nuclear-kafka-test]
   :group-id "nuclear-kafka-consumer-group"
   :key-deserializer keyword-deserializer
   :value-deserializer json-deserializer
   :shape [:map :topic :key :value :headers]})

(defn -main []
  (->> (->receiver receiver-cfg)
       (receive-auto-ack)
       (nk/on-next! println)
       (nk/block)))
