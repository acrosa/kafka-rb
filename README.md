# kafka-rb
kafka-rb allows you to produce and consume messages to / from the Kafka distributed messaging service.
This is an improved version of the original Ruby client written by Alejandro Crosa, 
and is used in production at wooga.

## Requirements
You need to have access to your Kafka instance and be able to connect through TCP. 
You can obtain a copy and instructions on how to setup kafka at http://incubator.apache.org/kafka/

To make Snappy compression available, add

    gem "snappy"

to your Gemfile.

## Installation

    sudo gem install kafka-rb

(should work fine with JRuby, Ruby 1.8 and 1.9)


## Usage

### Sending a simple message

    require 'kafka'
    producer = Kafka::Producer.new
    message = Kafka::Message.new("some random message content")
    producer.push(message)

### Sending a sequence of messages

    require 'kafka'
    producer = Kafka::Producer.new
    message1 = Kafka::Message.new("some random message content")
    message2 = Kafka::Message.new("some more content")
    producer.push([message1, message2])

### Batching a bunch of messages using the block syntax

    require 'kafka'
    producer = Kafka::Producer.new
    producer.batch do |messages|
        puts "Batching a push of multiple messages.."
        messages << Kafka::Message.new("first message to push")
        messages << Kafka::Message.new("second message to push")
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
    
## More real world example using partitions as well as reading avro

    #!/usr/bin/env ruby
    require 'kafka'
    require 'snappy'
    require 'avro'
    require 'json'
    
    # avro schema readers
    @schema = Avro::Schema.parse(File.open("events.avsc", "rb").read)
    @reader = Avro::IO::DatumReader.new(@schema)
    
    # converts bits to well defined avro object
    def to_event (bits)
        blob = StringIO.new(bits)
        decoder = Avro::IO::BinaryDecoder.new(blob)
        obj = @reader.read(decoder)
        puts obj.to_json
    end
    
    # define listener for each partition you have
    def subscribe (partition=0)
            trap(:INT) { exit }
            puts "polling partition #{partition}..."
            consumer = Kafka::Consumer.new({
                    :host => "YOUR_KAFKA_SERVER_HERE",
                    :port => 9092,
                    :topic => "events",
                    :max_size => 1024 * 1024 * 10,
                    :polling => 1,
                    :partition => partition })
            begin
                    consumer.loop do |messages|
                        puts "polling #{partition} partition..."
                        messages.each do |message|
                          to_event message.payload
                        end
                    end
            rescue
                    puts "Whoops, critical error #{$!} on partition #{partition}!!!"
            end
    end
    
    puts "Master listener started..."
    
    threads = []
    15.times do | p |
      threads[p] = Thread.new{ listen(p) }
    end
    
    threads.each do | t |
      t.join
    end

### Using the cli

There is two cli programs to communicate with kafka from the command line
interface mainly intended for debug.  `kafka-publish` and `kafka-consumer`. You
can configure the commands by command line arguments or by setting the
environment variables: *KAFKA_HOST*, *KAFKA_PORT*, *KAFKA_TOPIC*,
*KAFKA_COMPRESSION*.



#### kafka-publish

```
$ kafka-publish --help
Usage: kafka-publish [options]

    -h, --host HOST                  Set the kafka hostname
    -p, --port PORT                  Set the kafka port
    -t, --topic TOPIC                Set the kafka topic
    -c, --compression no|gzip|snappy Set the compression method
    -m, --message MESSAGE            Message to send
```

If _message_ is omitted, `kafka-publish` will read from *STDIN*, until EOF or
SIG-INT.

NOTE: kafka-publish doesn't bach messages for the moment.

This could be quiet handy for piping directly to kafka:

```
$ tail -f /var/log/syslog | kafka-publish -t syslog
```

#### kafka-consumer

```
$ kafka-consumer --help
Usage: kafka-consumer [options]

    -h, --host HOST                  Set the kafka hostname
    -p, --port PORT                  Set the kafka port
    -t, --topic TOPIC                Set the kafka topic
```

Kafka consumer will loop and wait for messages until it is interrupted.

This could be nice for example to have a sample of messages.


## Questions?
alejandrocrosa at(@) gmail.com
http://twitter.com/alejandrocrosa

tim.lossen@wooga.net
