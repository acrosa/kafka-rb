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

describe ProducerRequest do
  let(:message) { Kafka::Message.new }
  let(:req) { described_class.new("topic", message) }

  it "has a topic" do
    req.topic = "topic"
  end

  it "has a set of messages" do
    req.messages.should == [message]
  end

  it "has a default partition" do
    req.partition.should == 0
  end

  it "can use a user-specified partition" do
    req = described_class.new("topic", message, :partition => 42)
    req.partition.should == 42
  end
end
