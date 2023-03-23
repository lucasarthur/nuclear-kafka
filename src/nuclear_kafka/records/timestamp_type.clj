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
