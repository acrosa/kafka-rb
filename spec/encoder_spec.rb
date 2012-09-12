# encoding: utf-8

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
require File.dirname(__FILE__) + '/spec_helper'

describe Encoder do
  def check_message(bytes, message)
    encoded = [message.magic].pack("C") + [message.calculate_checksum].pack("N") + message.payload
    encoded = [encoded.length].pack("N") + encoded
    bytes.should == encoded
  end

  describe "Message Encoding" do
    it "should encode a message" do
      message = Kafka::Message.new("alejandro")
      check_message(described_class.message(message), message)
    end

    it "should encode an empty message" do
      message = Kafka::Message.new
      check_message(described_class.message(message), message)
    end

    it "should encode strings containing non-ASCII characters" do
      message = Kafka::Message.new("ümlaut")
      encoded = described_class.message(message)
      message = Kafka::Message.parse_from(encoded).messages.first
      if RUBY_VERSION[0,3] == "1.8" # Use old iconv on Ruby 1.8 for encoding
        ic = Iconv.new('UTF-8//IGNORE', 'UTF-8')
        ic.iconv(message.payload).should eql("ümlaut")
      else
        message.payload.force_encoding(Encoding::UTF_8).should eql("ümlaut")
      end
    end
  end

  describe "produce" do
    it "should binary encode an empty request" do
      bytes = described_class.produce("test", 0, [])
      bytes.length.should eql(20)
      bytes.should eql("\000\000\000\020\000\000\000\004test\000\000\000\000\000\000\000\000")
    end

    it "should binary encode a request with a message, using a specific wire format" do
      message = Kafka::Message.new("ale")
      bytes = described_class.produce("test", 3, message)
      data_size  = bytes[0, 4].unpack("N").shift
      request_id = bytes[4, 2].unpack("n").shift
      topic_length = bytes[6, 2].unpack("n").shift
      topic = bytes[8, 4]
      partition = bytes[12, 4].unpack("N").shift
      messages_length = bytes[16, 4].unpack("N").shift
      messages = bytes[20, messages_length]

      bytes.length.should eql(32)
      data_size.should eql(28)
      request_id.should eql(0)
      topic_length.should eql(4)
      topic.should eql("test")
      partition.should eql(3)
      messages_length.should eql(12)
    end
  end

  describe "multiproduce" do
    it "encodes an empty request" do
      bytes = described_class.multiproduce([])
      bytes.length.should == 8
      bytes.should == "\x00\x00\x00\x04\x00\x03\x00\x00"
    end

    it "encodes a request with a single topic/partition" do
      message = Kafka::Message.new("ale")
      bytes = described_class.multiproduce(Kafka::ProducerRequest.new("test", message))

      req_length = bytes[0, 4].unpack("N").shift
      req_type = bytes[4, 2].unpack("n").shift
      tp_count = bytes[6, 2].unpack("n").shift

      req_type.should == Kafka::RequestType::MULTIPRODUCE
      tp_count.should == 1

      topic_length = bytes[8, 2].unpack("n").shift
      topic = bytes[10, 4]
      partition = bytes[14, 4].unpack("N").shift
      messages_length = bytes[18, 4].unpack("N").shift
      messages_data = bytes[22, messages_length]

      topic_length.should == 4
      topic.should == "test"
      partition.should == 0
      messages_length.should == 12
      check_message(messages_data, message)
    end

    it "encodes a request with a single topic/partition but multiple messages" do
      messages = [Kafka::Message.new("ale"), Kafka::Message.new("beer")]
      bytes = described_class.multiproduce(Kafka::ProducerRequest.new("test", messages))

      req_length = bytes[0, 4].unpack("N").shift
      req_type = bytes[4, 2].unpack("n").shift
      tp_count = bytes[6, 2].unpack("n").shift

      req_type.should == Kafka::RequestType::MULTIPRODUCE
      tp_count.should == 1

      topic_length = bytes[8, 2].unpack("n").shift
      topic = bytes[10, 4]
      partition = bytes[14, 4].unpack("N").shift
      messages_length = bytes[18, 4].unpack("N").shift
      messages_data = bytes[22, messages_length]

      topic_length.should == 4
      topic.should == "test"
      partition.should == 0
      messages_length.should == 25
      check_message(messages_data[0, 12], messages[0])
      check_message(messages_data[12, 13], messages[1])
    end

    it "encodes a request with multiple topic/partitions" do
      messages = [Kafka::Message.new("ale"), Kafka::Message.new("beer")]
      bytes = described_class.multiproduce([
          Kafka::ProducerRequest.new("test", messages[0]),
          Kafka::ProducerRequest.new("topic", messages[1], partition: 1),
        ])

      req_length = bytes[0, 4].unpack("N").shift
      req_type = bytes[4, 2].unpack("n").shift
      tp_count = bytes[6, 2].unpack("n").shift

      req_type.should == Kafka::RequestType::MULTIPRODUCE
      tp_count.should == 2

      topic_length = bytes[8, 2].unpack("n").shift
      topic = bytes[10, 4]
      partition = bytes[14, 4].unpack("N").shift
      messages_length = bytes[18, 4].unpack("N").shift
      messages_data = bytes[22, 12]

      topic_length.should == 4
      topic.should == "test"
      partition.should == 0
      messages_length.should == 12
      check_message(messages_data[0, 12], messages[0])

      topic_length = bytes[34, 2].unpack("n").shift
      topic = bytes[36, 5]
      partition = bytes[41, 4].unpack("N").shift
      messages_length = bytes[45, 4].unpack("N").shift
      messages_data = bytes[49, 13]

      topic_length.should == 5
      topic.should == "topic"
      partition.should == 1
      messages_length.should == 13
      check_message(messages_data[0, 13], messages[1])
    end
  end
end
