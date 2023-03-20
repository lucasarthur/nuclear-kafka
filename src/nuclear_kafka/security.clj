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

(ns nuclear-kafka.security)

(def protocols {:plain "PLAINTEXT"
                :ssl "SSL"
                :sasl-plain "SASL_PLAINTEXT"
                :sasl-ssl "SASL_SSL"})

(def mechanisms
  {:plain {:name "PLAIN"
           :module "org.apache.kafka.common.security.plain.PlainLoginModule"}
   :sha-256 {:name "SCRAM-SHA-256"
             :module "org.apache.kafka.common.security.scram.ScramLoginModule"}
   :sha-512 {:name "SCRAM-SHA-512"
             :module "org.apache.kafka.common.security.scram.ScramLoginModule"}})
