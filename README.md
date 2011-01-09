# kafka-rb
kafka-rb allows you to produce messages to the Kafka distributed publish/subscribe messaging service.

## Requirements
You need to have access to your Kafka instance and be able to connect through TCP. You can obtain a copy and instructions on how to setup kafka at 

## Installation
sudo gem install kafka-rb

(the code works fine with JRuby, Ruby 1.8x and Ruby 1.9.x)

## Usage

### Sending a simple message

require 'kafka-rb'

producer = Kafka::Producer.new
message = Kafka::Message.new("some random message content")
producer.send(message)

### sending a sequence of messages

require 'kafka-rb'

producer = Kafka::Producer.new
message1 = Kafka::Message.new("some random message content")
message2 = Kafka::Message.new("some more content")
producer.send([message1, message2])

### batching a bunch of messages using the block syntax

require 'kafka-rb'

producer = Kafka::Producer.new
producer.batch do |messages|
  puts "Batching a send of multiple messages.."
  messages << Kafka::Message.new("first message to send")
  messages << Kafka::Message.new("second message to send")
end

* they will be sent all at once, after the block execution
