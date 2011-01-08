# kafka-rb
kafka-rb allows you to produce messages to the Kafka distributed publish/subscribe messaging service.

## Requirements
You need to have access to your Kafka instance and be able to connect through TCP. You can obtain a copy and instructions on how to setup kafka at 

## Installation
sudo gem install kafka-rb

## Usage

### Sending a simple message

require 'kafka-rb'

producer = Kafka::Producer.new
message = Kafka::Message.new("some random message content")
producer.send(message)

### batching a sequence of messages

require 'kafka-rb'

producer = Kafka::Producer.new
message1 = Kafka::Message.new("some random message content")
message2 = Kafka::Message.new("some more content")
producer.send([message1, message2])

### or you can also use the block batching syntax

require 'kafka-rb'

producer = Kafka::Producer.new
producer.batch do |batch|
  puts "Batching a send of multiple messages.."
  batch << Kafka::Message.new("some random message content")
  batch << Kafka::Message.new("some more content")
end

