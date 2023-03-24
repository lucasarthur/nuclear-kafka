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

(ns nuclear-kafka.records.timestamp-type
  (:refer-clojure :exclude [name])
  (:require
   [clojure.string :as str])
  (:import
   (org.apache.kafka.common.record TimestampType)))

(def no-timestamp-type (TimestampType/NO_TIMESTAMP_TYPE))
(def create-time (TimestampType/CREATE_TIME))
(def log-append-time (TimestampType/LOG_APPEND_TIME))

(defn- pascal->kebab [s]
  (-> (str/replace s #"(?<!^)([A-Z][a-z]|(?<=[a-z])[A-Z0-9])" "-$1")
      (str/trim)
      (str/lower-case)))

(defn id [timestamp-type]
  (.-id timestamp-type))

(defn name [timestamp-type]
  (-> timestamp-type .-name pascal->kebab keyword))

(def timestamp-types
  {no-timestamp-type
   {:id (id no-timestamp-type)
    :name (name no-timestamp-type)}
   create-time
   {:id (id create-time)
    :name (name create-time)}
   log-append-time
   {:id (id log-append-time)
    :name (name log-append-time)}})
