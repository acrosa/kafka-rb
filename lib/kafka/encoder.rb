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

module Kafka
  module Encoder
    def self.message(message, compression = Message::NO_COMPRESSION)
      message.encode(compression)
    end

    def self.message_block(topic, partition, messages, compression)
      message_set = message_set(messages, compression)

      topic     = [topic.length].pack("n") + topic
      partition = [partition].pack("N")
      messages  = [message_set.length].pack("N") + message_set

      return topic + partition + messages
    end

    def self.message_set(messages, compression)
      message_set = Array(messages).collect { |message|
        self.message(message)
      }.join("")
      message_set = self.message(Message.new(message_set), compression) unless compression == Message::NO_COMPRESSION
      message_set
    end

    def self.produce(topic, partition, messages, compression = Message::NO_COMPRESSION)
      request = [RequestType::PRODUCE].pack("n")
      data = request + self.message_block(topic, partition, messages, compression)

      return [data.length].pack("N") + data
    end

    def self.multiproduce(producer_requests, compression = Message::NO_COMPRESSION)
      part_set = Array(producer_requests).map { |req|
        self.message_block(req.topic, req.partition, req.messages, compression)
      }

      request = [RequestType::MULTIPRODUCE].pack("n")
      parts = [part_set.length].pack("n") + part_set.join("")
      data = request + parts

      return [data.length].pack("N") + data
    end
  end
end
