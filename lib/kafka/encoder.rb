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
    def self.message(message)
      payload = \
        if RUBY_VERSION[0,3] == "1.8"  # Use old iconv on Ruby 1.8 for encoding
          Iconv.new('UTF-8//IGNORE', 'UTF-8').iconv(message.payload.to_s)
        else
          message.payload.to_s.force_encoding(Encoding::ASCII_8BIT)
        end
      data = [message.magic].pack("C") + [message.calculate_checksum].pack("N") + payload

      [data.length].pack("N") + data
    end

    def self.message_block(topic, partition, messages)
      message_set = Array(messages).collect { |message|
        self.message(message)
      }.join("")

      topic     = [topic.length].pack("n") + topic
      partition = [partition].pack("N")
      messages  = [message_set.length].pack("N") + message_set

      return topic + partition + messages
    end

    def self.produce(topic, partition, messages)
      request = [RequestType::PRODUCE].pack("n")
      data = request + self.message_block(topic, partition, messages)

      return [data.length].pack("N") + data
    end

    def self.multiproduce(producer_requests)
      part_set = Array(producer_requests).map { |req|
        self.message_block(req.topic, req.partition, req.messages)
      }

      request = [RequestType::MULTIPRODUCE].pack("n")
      parts = [part_set.length].pack("n") + part_set.join("")
      data = request + parts

      return [data.length].pack("N") + data
    end
  end
end
