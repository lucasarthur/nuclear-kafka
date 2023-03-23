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

(ns nuclear-kafka.config.consumer
  (:require
   [nuclear-kafka.config.common :refer [->kafka-cfg-map]]
   [nuclear-kafka.deserializer :refer [->deserializer]]
   [nuclear.util :refer [ms->duration]]
   [nuclear.util.sam :refer [->consumer ->supplier]]
   [nuclear.util.schedulers :refer [immediate]])
  (:import
   (reactor.kafka.receiver ReceiverOptions ReceiverOptions$ConsumerListener)))

(defn ->consumer-listener [on-consumer-added on-consumer-removed]
  (reify
    ReceiverOptions$ConsumerListener
    (consumerAdded [_ id consumer] (on-consumer-added id consumer))
    (consumerRemoved [_ id consumer] (on-consumer-removed id consumer))))

(defn- add-assign-listeners [opts assign-listeners]
  (reduce #(.addAssignListener %1 (->consumer %2)) opts assign-listeners))

(defn- add-revoke-listeners [opts revoke-listeners]
  (reduce #(.addRevokeListener %1 (->consumer %2)) opts revoke-listeners))

(defn- add-consumer-listener [opts consumer-listener]
  (if-not (empty? consumer-listener)
    (.consumerListener
     opts
     (->consumer-listener
      (or (:on-consumer-added consumer-listener) (fn [_ _] ()))
      (or (:on-consumer-removed consumer-listener) (fn [_ _] ()))))
    opts))

(defn- ->consumer-cfg-map [options auto-offset-reset group-id]
  (->kafka-cfg-map (update-in
                    options [:options]
                    #(assoc
                      %
                      "auto.offset.reset" (name auto-offset-reset)
                      "group.id" group-id))))

(defn clear-assign-listeners [opts]
  (.clearAssignListeners opts))

(defn clear-revoke-listeners [opts]
  (.clearRevokeListeners opts))

(defn ->consumer-options
  [{:keys [key-deserializer
           value-deserializer
           topics
           group-id
           auto-offset-reset
           poll-timeout
           close-timeout
           assign-listeners
           revoke-listeners
           commit-interval
           commit-batch-size
           max-commit-attempts
           commit-retry-interval
           on-scheduler
           consumer-listener]
    :or {key-deserializer :byte-array
         value-deserializer :byte-array
         auto-offset-reset :earliest
         poll-timeout 100
         close-timeout Long/MAX_VALUE
         commit-interval 5000
         commit-batch-size 0
         max-commit-attempts 100
         commit-retry-interval 500
         on-scheduler immediate}
    :as options}]
  (when-not group-id (throw (ex-info "A consumer MUST have a group id!" {})))
  (when-not topics (throw (ex-info "A consumer MUST listen to some topic!" {})))
  (when (and (vector? topics) (empty? topics))
    (throw (ex-info "A consumer MUST listen to at least ONE topic!" {})))
  (-> (ReceiverOptions/create (->consumer-cfg-map options auto-offset-reset group-id))
      (.withKeyDeserializer (->deserializer key-deserializer))
      (.withValueDeserializer (->deserializer value-deserializer))
      (.pollTimeout (ms->duration poll-timeout))
      (.closeTimeout (ms->duration close-timeout))
      (add-assign-listeners assign-listeners)
      (add-revoke-listeners revoke-listeners)
      (.commitInterval (ms->duration commit-interval))
      (.commitBatchSize commit-batch-size)
      (.maxCommitAttempts max-commit-attempts)
      (.commitRetryInterval (ms->duration commit-retry-interval))
      (.schedulerSupplier (->supplier (fn [] on-scheduler)))
      (add-consumer-listener consumer-listener)
      (.subscription topics)))

;; add shape option here
