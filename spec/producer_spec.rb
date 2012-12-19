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

describe Producer do

  before(:each) do
    @mocked_socket = mock(TCPSocket)
    TCPSocket.stub!(:new).and_return(@mocked_socket) # don't use a real socket
    @producer = Producer.new
  end

  describe "Kafka Producer" do
    it "should have a topic and a partition" do
      @producer.should respond_to(:topic)
      @producer.should respond_to(:partition)
    end

    it "should have compression" do
      @producer.should respond_to :compression
      Producer.new(:compression => 1).compression.should == 1
      Producer.new.compression.should == 0
    end

    it "should set a topic and partition on initialize" do
      @producer = Producer.new({ :host => "localhost", :port => 9092, :topic => "testing" })
      @producer.topic.should eql("testing")
      @producer.partition.should eql(0)
      @producer = Producer.new({ :topic => "testing", :partition => 3 })
      @producer.partition.should eql(3)
    end

    it "should set default host and port if none is specified" do
      @producer = Producer.new
      @producer.host.should eql("localhost")
      @producer.port.should eql(9092)
    end
  end

  it "should send messages" do
    @producer.should_receive(:write).and_return(32)
    message = Kafka::Message.new("ale")
    @producer.send(message).should eql(32)
  end

  describe "Message Batching" do
    it "should batch messages and send them at once" do
      message1 = Kafka::Message.new("one")
      message2 = Kafka::Message.new("two")
      @producer.should_receive(:send).with([message1, message2]).exactly(:once).and_return(nil)
      @producer.batch do |messages|
        messages << message1
        messages << message2
      end
    end
  end
end
