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

(ns nuclear-kafka.serializer
  (:require [cheshire.core :as json])
  (:import [org.apache.kafka.common.serialization
            Serializer
            ByteArraySerializer
            ByteBufferSerializer
            LongSerializer
            StringSerializer]))

(defn serializer [f]
  (reify
    Serializer
    (close [_])
    (configure [_ _ _])
    (serialize [_ topic payload]
      (f topic payload))))

(defn edn-serializer ^Serializer []
  (serializer
   (fn [_ payload] (some-> payload pr-str .getBytes))))

(defn json-serializer ^Serializer []
  (serializer
   (fn [_ payload] (some-> payload json/generate-string .getBytes))))

(defn keyword-serializer ^Serializer []
  (serializer (fn [_ k] (some-> k name .getBytes))))

(defn byte-array-serializer ^ByteArraySerializer []
  (ByteArraySerializer.))

(defn byte-buffer-serializer ^ByteBufferSerializer []
  (ByteBufferSerializer.))

(defn long-serializer ^LongSerializer []
  (LongSerializer.))

(defn string-serializer ^StringSerializer []
  (StringSerializer.))

(def serializers
  {:byte-array  byte-array-serializer
   :byte-buffer byte-buffer-serializer
   :keyword     keyword-serializer
   :edn         edn-serializer
   :json        json-serializer
   :long        long-serializer
   :string      string-serializer})

(defn ->serializer ^Serializer [x]
  (cond
    (keyword? x) (if-let [f (serializers x)]
                   (f)
                   (throw (ex-info "unknown serializer alias" {})))
    (fn? x) (x)
    :else  x))
