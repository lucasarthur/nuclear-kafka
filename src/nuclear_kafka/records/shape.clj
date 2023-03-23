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

(ns nuclear-kafka.records.shape)

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

(defmulti with-shape shape-dispatcher)

(defmethod with-shape :value [_ record] (:value record))

(defmethod with-shape :vector [shape record]
  (->> (subvec shape 1) (vector-shape-reducer record)))

(defmethod with-shape :map [shape record]
  (->> (subvec shape 1) (map-shape-reducer record)))

(defmethod with-shape :default [_ record] record)
