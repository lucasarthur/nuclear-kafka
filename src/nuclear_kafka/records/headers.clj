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

(ns nuclear-kafka.records.headers
  (:refer-clojure :exclude [key remove])
  (:import
   (org.apache.kafka.common.header.internals RecordHeader RecordHeaders)))

(defn ->record-header
  ([[k v]] (->record-header k v))
  ([k v] (RecordHeader. (name k) (.getBytes v "UTF-8"))))

(defn key [header]
  (-> header .key keyword))

(defn value [header]
  (.value header))

(defn header->pair [header]
  [(key header) (-> header value (String. "UTF-8"))])

(defn pairs->map [pairs]
  (reduce (fn [m [k vs]] (update m k #(conj (or % []) vs))) {} pairs))

(defn add
  ([k v headers] (add [k v] headers))
  ([header headers] (->> header ->record-header (.add headers))))

(defn remove [key headers]
  (->> key name (.remove headers)))

(defn last-header [key headers]
  (->> key name (.lastHeader headers) header->pair))

(defn header->map [key headers]
  (->> (name key) (.headers headers) (map header->pair) (pairs->map)))

(defn headers->map [headers]
  (->> headers (map header->pair) (pairs->map)))

(defn map->single-value-map
  ([m] (map->single-value-map true m))
  ([last? m] (reduce-kv #(assoc %1 %2 ((if last? last first) %3)) {} m)))

(defn header->single-value-map
  ([key headers] (header->single-value-map key true headers))
  ([key last? headers]
   (->> headers (header->map key) (map->single-value-map last?))))

(defn headers->single-value-map
  ([headers] (headers->single-value-map true headers))
  ([last? headers]
   (->> headers headers->map (map->single-value-map last?))))

(defn ->record-headers [headers]
  (reduce #(add %2 %1) (RecordHeaders.) headers))

(defn ->record-header-seq [header-map]
  (when (seq header-map) (map ->record-header header-map)))
