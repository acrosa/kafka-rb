# kafka-rb
kafka-rb allows you to produce and consume messages to / from the Kafka distributed messaging service.
This is an improved version of the original Ruby client written by Alejandro Crosa, 
and is used in production at wooga.

## Requirements
You need to have access to your Kafka instance and be able to connect through TCP. 
You can obtain a copy and instructions on how to setup kafka at http://incubator.apache.org/kafka/


## Installation

    sudo gem install kafka-rb

(should work fine with JRuby, Ruby 1.8 and 1.9)


## Usage

### Sending a simple message

    require 'kafka'
    producer = Kafka::Producer.new
    message = Kafka::Message.new("some random message content")
    producer.send(message)

### Sending a sequence of messages

    require 'kafka'
    producer = Kafka::Producer.new
    message1 = Kafka::Message.new("some random message content")
    message2 = Kafka::Message.new("some more content")
    producer.send([message1, message2])

### Batching a bunch of messages using the block syntax

    require 'kafka'
    producer = Kafka::Producer.new
    producer.batch do |messages|
        puts "Batching a send of multiple messages.."
        messages << Kafka::Message.new("first message to send")
        messages << Kafka::Message.new("second message to send")
    end

* they will be sent all at once, after the block execution

### Consuming messages one by one

    require 'kafka'
    consumer = Kafka::Consumer.new
    messages = consumer.consume


### Consuming messages using a block loop

    require 'kafka'
    consumer = Kafka::Consumer.new
    consumer.loop do |messages|
        puts "Received"
        puts messages
    end


## Questions?
alejandrocrosa at(@) gmail.com
http://twitter.com/alejandrocrosa

tim.lossen@wooga.net
