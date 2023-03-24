# Nuclear Kafka

A Clojure wrapper with batteries for Reactor Kafka.

## Configuration

### Producer config sample

```clj
{:brokers ["localhost" 9092]
 :key-serializer keyword-serializer
 :value-serializer json-serializer
 :shape [:vector :topic :key :value :headers]
 :max-in-flight 10
 :stop-on-error? false
 :on-scheduler bounded-elastic
 :producer-listener
 {:on-producer-added #(println "id: " %1 "\nproducer: " %2)
  :on-producer-removed #(println "id: " %1 "\nproducer: " %2)}
 :protocol :sasl-plain
 :auth {:username "lucas"
        :password "my-pwd"
        :mechanism :plain}}
```

### Producer config sample

```clj
{:brokers ["localhost" 9092]
 :topics [:nuclear-kafka-test]
 :group-id "nuclear-kafka-consumer-group"
 :key-deserializer keyword-deserializer
 :value-deserializer json-deserializer
 :shape [:map :topic :key :value :headers]
 :poll-timeout 1000
 :close-timeout 20000
 :assign-listeners [println]
 :revoke-listeners [println]
 :commit-interval 3000
 :commit-batch-size 5
 :max-commit-attempts 500
 :commit-retry-interval 1000
 :on-scheduler bounded-elastic
 :consumer-listener
 {:on-consumer-added #(println "id: " %1 "\nconsumer: " %2)
  :on-consumer-removed #(println "id: " %1 "\nconsumer: " %2)}
 :protocol :sasl-plain
 :auth {:username "lucas"
        :password "my-pwd"
        :mechanism :plain}}
```

## Copyright & License

Copyright (c) 2023 Lucas Arthur

This file is part of Nuclear Kafka, an open-source library whose goal is to
use Reactor Kafka with Clojure in an idiomatic and simplified way.

Nuclear Kafka is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

Nuclear Kafka is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with Nuclear Kafka. If not, see <http://www.gnu.org/licenses/>.
