# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'optparse'

module Kafka
  module CLI #:nodoc: all
    extend self

    def publish!
      read_env
      parse_args
      validate_config
      if config[:message]
        push(config, config.delete(:message))
      else
        publish(config)
      end
    end


    def subscribe!
      read_env
      parse_args
      validate_config
      subscribe(config)
    end

    def validate_config
      if config[:help]
        puts help
        exit
      end
      config[:host] ||= IO::HOST
      config[:port] ||= IO::PORT
      config[:topic].is_a?(String) or raise "Missing topic"

    rescue RuntimeError => e
      puts e.message
      puts help
      exit
    end

    def parse_args(args = ARGV)
      option_parser.parse(args)
    end

    def read_env(env = ENV)
      config[:host] = env["KAFKA_HOST"] if env["KAFKA_HOST"]
      config[:port] = env["KAFKA_PORT"].to_i if env["KAFKA_PORT"]
      config[:topic] = env["KAFKA_TOPIC"] if env["KAFKA_TOPIC"]
      config[:compression] = string_to_compression(env["KAFKA_COMPRESSION"]) if env["KAFKA_COMPRESSION"]
    end

    def config
      @config ||= {:compression => string_to_compression("no")}
    end

    def help
      option_parser.to_s
    end

    def option_parser
      OptionParser.new do |opts|
        opts.banner = "Usage: #{program_name} [options]"
        opts.separator ""

        opts.on("-h","--host HOST", "Set the kafka hostname") do |h|
          config[:host] = h
        end

        opts.on("-p", "--port PORT", "Set the kafka port") do |port|
          config[:port] = port.to_i
        end

        opts.on("-t", "--topic TOPIC", "Set the kafka topic") do |topic|
          config[:topic] = topic
        end

        opts.on("-c", "--compression no|gzip|snappy", "Set the compression method") do |meth|
          config[:compression] = string_to_compression(meth)
        end if publish?

        opts.on("-m","--message MESSAGE", "Message to send") do |msg|
          config[:message] = msg
        end if publish?

        opts.separator ""

        opts.on("--help", "show the help") do
          config[:help] = true
        end

        opts.separator ""
        opts.separator "You can set the host, port, topic and compression from the environment variables: KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC AND KAFKA_COMPRESSION"
      end
    end

    def publish?
      program_name == "kafka-publish"
    end

    def subscribe?
      program_name == "kafka-subscribe"
    end

    def program_name(pn = $0)
      File.basename(pn)
    end

    def string_to_compression(meth)
      case meth
      when "no" then Message::NO_COMPRESSION
      when "gzip" then Message::GZIP_COMPRESSION
      when "snappy" then Message::SNAPPY_COMPRESSION
      else raise "No supported compression"
      end
    end

    def push(options, message)
      Producer.new(options).push(Message.new(message))
    end

    def publish(options)
      trap(:INT){ exit }
      producer = Producer.new(options)
      loop do
        publish_loop(producer)
      end
    end

    def publish_loop(producer)
      message = read_input
      producer.push(Message.new(message))
    end

    def read_input
      input = $stdin.gets
      if input
        input.strip
      else
        exit # gets return nil when eof
      end

    end

    def subscribe(options)
      trap(:INT){ exit }
      consumer = Consumer.new(options)
      consumer.loop do |messages|
        messages.each do |message|
          puts message.payload
        end
      end
    end

  end
end
