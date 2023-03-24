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

(ns nuclear-kafka.records.shape
  (:require
   [nuclear-kafka.records.producer.sender-record :refer [->sender-record]]))

(defn- shape-dispatcher [shape _]
  (if (sequential? shape) (first shape) shape))

(defn- map-shape-reducer [rec shape]
  (reduce
   #(if (vector? %2)
      (let [nested-key (first %2)
            nested-shape (subvec %2 1)]
        (assoc %1 nested-key (map-shape-reducer (nested-key rec) nested-shape)))
      (assoc %1 %2 (%2 rec)))
   {}
   shape))

(defn- vector-shape-reducer [rec shape]
  (reduce
   #(if (vector? %2)
      (let [nested-key (first %2)
            nested-shape (subvec %2 1)]
        (conj %1 (vector-shape-reducer (nested-key rec) nested-shape)))
      (conj %1 (%2 rec)))
   []
   shape))

(defmulti consumer-shape shape-dispatcher)

(defmethod consumer-shape :value [_ record] (:value record))

(defmethod consumer-shape :vector [shape record]
  (->> (subvec shape 1) (vector-shape-reducer record)))

(defmethod consumer-shape :map [shape record]
  (->> (subvec shape 1) (map-shape-reducer record)))

(defmethod consumer-shape :default [_ record] record)

(defmulti producer-shape shape-dispatcher)

(defmethod producer-shape :topic-value [_ record]
  (->sender-record
   (if (map? record)
     (select-keys record [:topic :value])
     {:topic (first record) :value (second record)})))

(defmethod producer-shape :default [_ record] (producer-shape :topic-value record))

(defmethod producer-shape :map [shape record]
  (->> (if (vector? shape) (subvec shape 1) [])
       (#(if (empty? %) record (select-keys record %)))
       (->sender-record)))

(defmethod producer-shape :vector [shape record]
  (when-not (vector? shape)
    (throw
     (IllegalArgumentException.
      "MUST specify vector fields!")))
  (-> (subvec shape 1)
      (zipmap record)
      (->sender-record)))
