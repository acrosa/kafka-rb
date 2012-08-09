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

describe Message do

  before(:each) do
    @message = Message.new
  end

  describe "Kafka Message" do
    it "should have a default magic number" do
      Message::MAGIC_IDENTIFIER_DEFAULT.should eql(0)
    end

    it "should have a magic field, a checksum and a payload" do
      [:magic, :checksum, :payload].each do |field|
        @message.should respond_to(field.to_sym)
      end
    end

    it "should set a default value of zero" do
      @message.magic.should eql(Kafka::Message::MAGIC_IDENTIFIER_DEFAULT)
    end

    it "should allow to set a custom magic number" do
      @message = Message.new("ale", 1)
      @message.magic.should eql(1)
    end

    it "should calculate the checksum (crc32 of a given message)" do
      @message.payload = "ale"
      @message.calculate_checksum.should eql(1120192889)
      @message.payload = "alejandro"
      @message.calculate_checksum.should eql(2865078607)
    end

    it "should say if the message is valid using the crc32 signature" do
      @message.payload  = "alejandro"
      @message.checksum = 2865078607
      @message.valid?.should eql(true)
      @message.checksum = 0
      @message.valid?.should eql(false)
      @message = Message.new("alejandro", 0, 66666666) # 66666666 is a funny checksum
      @message.valid?.should eql(false)
    end
  end

  describe "parsing" do
    it "should parse a version-0 message from bytes" do
      bytes = [8, 0, 1120192889, 'ale'].pack('NCNa*')
      message = Kafka::Message.parse_from(bytes).messages.first
      message.valid?.should eql(true)
      message.magic.should eql(0)
      message.checksum.should eql(1120192889)
      message.payload.should eql("ale")
    end

    it "should parse a version-1 message from bytes" do
      bytes = [12, 1, 0, 755095536, 'martin'].pack('NCCNa*')
      message = Kafka::Message.parse_from(bytes).messages.first
      message.should be_valid
      message.magic.should == 1
      message.checksum.should == 755095536
      message.payload.should == 'martin'
    end

    it "should raise an error if the magic number is not recognised" do
      bytes = [12, 2, 0, 755095536, 'martin'].pack('NCCNa*') # 2 = some future format that's not yet invented
      lambda {
        Kafka::Message.parse_from(bytes)
      }.should raise_error(RuntimeError, /Unsupported Kafka message version/)
    end

    it "should skip an incomplete message at the end of the response" do
      bytes = [8, 0, 1120192889, 'ale'].pack('NCNa*')
      bytes += [8].pack('N') # incomplete message (only length, rest is truncated)
      message_set = Message.parse_from(bytes)
      message_set.messages.size.should == 1
      message_set.size.should == 12 # bytes consumed
    end

    it "should skip an incomplete message at the end of the response which has the same length as an empty message" do
      bytes = [8, 0, 1120192889, 'ale'].pack('NCNa*')
      bytes += [8, 0, 1120192889].pack('NCN') # incomplete message (payload is missing)
      message_set = Message.parse_from(bytes)
      message_set.messages.size.should == 1
      message_set.size.should == 12 # bytes consumed
    end

    it "should read empty messages correctly" do
      # empty message
      bytes = [5, 0, 0, ''].pack('NCNa*')
      messages = Message.parse_from(bytes).messages
      messages.size.should == 1
      messages.first.payload.should == ''
    end

    it "should parse a gzip-compressed message" do
      compressed = 'H4sIAG0LI1AAA2NgYBBkZBB/9XN7YlJRYnJiCogCAH9lueQVAAAA'.unpack('m*').shift
      bytes = [45, 1, 1, 1303540914, compressed].pack('NCCNa*')
      message = Message.parse_from(bytes).messages.first
      message.should be_valid
      message.payload.should == 'abracadabra'
    end

    it "should recursively parse nested compressed messages" do
      uncompressed = [17, 1, 0, 401275319, 'abracadabra'].pack('NCCNa*')
      uncompressed << [12, 1, 0, 2666930069, 'foobar'].pack('NCCNa*')
      compressed_io = StringIO.new('')
      Zlib::GzipWriter.new(compressed_io).tap{|gzip| gzip << uncompressed; gzip.close }
      compressed = compressed_io.string
      bytes = [compressed.size + 6, 1, 1, Zlib.crc32(compressed), compressed].pack('NCCNa*')
      messages = Message.parse_from(bytes).messages
      messages.map(&:payload).should == ['abracadabra', 'foobar']
      messages.map(&:valid?).should == [true, true]
    end

    it "should support a mixture of compressed and uncompressed messages" do
      compressed = 'H4sIAG0LI1AAA2NgYBBkZBB/9XN7YlJRYnJiCogCAH9lueQVAAAA'.unpack('m*').shift
      bytes = [45, 1, 1, 1303540914, compressed].pack('NCCNa*')
      bytes << [11, 1, 0, 907060870, 'hello'].pack('NCCNa*')
      messages = Message.parse_from(bytes).messages
      messages.map(&:payload).should == ['abracadabra', 'hello']
      messages.map(&:valid?).should == [true, true]
    end

    it "should raise an error if the compression codec is not supported" do
      bytes = [6, 1, 2, 0, ''].pack('NCCNa*') # 2 = Snappy codec
      lambda {
        Kafka::Message.parse_from(bytes)
      }.should raise_error(RuntimeError, /Unsupported Kafka compression codec/)
    end
  end
end
