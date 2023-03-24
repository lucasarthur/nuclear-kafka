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

(ns nuclear-kafka.deserializer
  (:require
   [clojure.java.io :as io]
   [clojure.edn :as edn]
   [cheshire.core :as json]
   [clojure.string :as str])
  (:import
   (org.apache.kafka.common.serialization
    Deserializer
    ByteArrayDeserializer
    ByteBufferDeserializer
    LongDeserializer
    StringDeserializer)))

(defn deserializer [f]
  (reify
    Deserializer
    (close [_])
    (configure [_ _ _])
    (deserialize [_ topic payload]
      (f topic payload))))

(defn edn-deserializer
  (^Deserializer []
   (edn-deserializer {:eof nil}))
  (^Deserializer [opts]
   (deserializer
    (fn [_ ^bytes payload]
      (some-> payload (String. "UTF-8") (->> (edn/read-string opts)))))))

(defn json-deserializer ^Deserializer []
  (deserializer
   (fn [_ ^bytes payload]
     (some-> payload io/reader (json/parse-stream true)))))

(defn keyword-deserializer ^Deserializer []
  (deserializer (fn [_ ^bytes k]
                  (let [key-str (String. k "UTF-8")]
                    (when-not (str/blank? key-str)
                      (keyword key-str))))))

(defn byte-array-deserializer ^ByteArrayDeserializer []
  (ByteArrayDeserializer.))

(defn byte-buffer-deserializer ^ByteBufferDeserializer []
  (ByteBufferDeserializer.))

(defn long-deserializer ^LongDeserializer []
  (LongDeserializer.))

(defn string-deserializer ^StringDeserializer []
  (StringDeserializer.))

(def deserializers
  {:byte-array  byte-array-deserializer
   :byte-buffer byte-buffer-deserializer
   :keyword     keyword-deserializer
   :edn         edn-deserializer
   :json        json-deserializer
   :long        long-deserializer
   :string      string-deserializer})

(defn ->deserializer ^Deserializer [x]
  (cond
    (keyword? x) (if-let [f (deserializers x)]
                   (f)
                   (throw (ex-info "unknown deserializer alias" {:type :unknown-deserializer})))
    (fn? x) (x)
    :else x))
