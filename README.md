# Requirements
You need to have access to your Kafka instance and be able to connect through TCP. You can obtain a copy and instructions on how to setup kafka at 

# Installation
sudo gem install kafka-rb

# Usage
require 'kafka-rb'

producer = Kafka::Producer.new

message = Kafka::Message.new("some random message content")

producer.send(message)

