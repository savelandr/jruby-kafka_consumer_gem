#Kafka Consumer
##Description
Helper to read messages from a Kafka cluster
##Example
```
require 'jruby/kafka_consumer'

string_consumer=KafkaConsumer.new("zk.host.aol.com:2181/path", "my-topic", :string, :string)
string_consumer.get_message
string_consumer.close

byte_ary_consumer=KafkaConsumer.new("zk.host.aol.com:2181/path", "my-topic2", :byte_array, :byte_array, "auto.offset.reset" => "largest")
byte_ary_consumer.get_message
byte_ary_consumer.close
```
