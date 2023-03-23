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

(ns nuclear-kafka.config.producer
  (:require
   [nuclear-kafka.config.common :refer [->kafka-cfg-map]]
   [nuclear-kafka.serializer :refer [->serializer]]
   [nuclear.util :refer [ms->duration]]
   [nuclear.util.schedulers :refer [immediate]])
  (:import
   (reactor.util.concurrent Queues)
   (reactor.kafka.sender SenderOptions SenderOptions$ProducerListener)))

(defn ->producer-listener [on-producer-added on-producer-removed]
  (reify
    SenderOptions$ProducerListener
    (producerAdded [_ id producer] (on-producer-added id producer))
    (producerRemoved [_ id producer] (on-producer-removed id producer))))

(defn- add-producer-listener [opts producer-listener]
  (if-not (empty? producer-listener)
    (.producerListener
     opts
     (->producer-listener
      (or (:on-producer-added producer-listener) (fn [_ _] ()))
      (or (:on-producer-removed producer-listener) (fn [_ _] ()))))
    opts))

(defn ->producer-options
  [{:keys [key-serializer
           value-serializer
           max-in-flight
           stop-on-error?
           close-timeout
           on-scheduler
           producer-listener]
    :or {key-serializer :byte-array
         value-serializer :byte-array
         close-timeout Long/MAX_VALUE
         max-in-flight Queues/SMALL_BUFFER_SIZE
         stop-on-error? true
         on-scheduler immediate}
    :as options}]
  (-> (SenderOptions/create (->kafka-cfg-map options))
      (.withKeySerializer (->serializer key-serializer))
      (.withValueSerializer (->serializer value-serializer))
      (.maxInFlight max-in-flight)
      (.stopOnError stop-on-error?)
      (.closeTimeout (ms->duration close-timeout))
      (.scheduler on-scheduler)
      (add-producer-listener producer-listener)))

;; add transactions

;; Producer
;; {:brokers ["localhost" 9092]
;;  :key-serializer long-serializer
;;  :value-serializer json-serializer
;;  :max-in-flight 10
;;  :stop-on-error? false
;;  :on-scheduler bounded-elastic
;;  :producer-listener
;;  {:on-producer-added #(println "id: " %1 "\nproducer: " %2)
;;   :on-producer-removed #(println "id: " %1 "\nproducer: " %2)}
;;  :protocol :sasl-plain
;;  :auth {:username "lucas"
;;         :password "arthur0102"
;;         :mechanism :plain}}

;; Producer minimal config
;; {:brokers ["localhost" 9092]
;;  :key-serializer long-serializer
;;  :value-serializer json-serializer
;;  :protocol :sasl-plain
;;  :auth {:username "lucas"
;;         :password "arthur0102"
;;         :mechanism :plain}}
