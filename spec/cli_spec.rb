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
require 'kafka/cli'

describe CLI do

  before(:each) do
    CLI.instance_variable_set("@config", {})
    CLI.stub(:puts)
  end

  describe "should read from env" do
    describe "kafka host" do
      it "should read KAFKA_HOST from env" do
        CLI.read_env("KAFKA_HOST" => "google.com")
        CLI.config[:host].should == "google.com"
      end

      it "kafka port" do
        CLI.read_env("KAFKA_PORT" => "1234")
        CLI.config[:port].should == 1234
      end

      it "kafka topic" do
        CLI.read_env("KAFKA_TOPIC" => "news")
        CLI.config[:topic].should == "news"
      end

      it "kafka compression" do
        CLI.read_env("KAFKA_COMPRESSION" => "no")
        CLI.config[:compression].should == Message::NO_COMPRESSION

        CLI.read_env("KAFKA_COMPRESSION" => "gzip")
        CLI.config[:compression].should == Message::GZIP_COMPRESSION

        CLI.read_env("KAFKA_COMPRESSION" => "snappy")
        CLI.config[:compression].should == Message::SNAPPY_COMPRESSION
      end
    end
  end

  describe "should read from command line" do
    it "kafka host" do
      CLI.parse_args(%w(--host google.com))
      CLI.config[:host].should == "google.com"

      CLI.parse_args(%w(-h google.com))
      CLI.config[:host].should == "google.com"
    end

    it "kafka port" do
      CLI.parse_args(%w(--port 1234))
      CLI.config[:port].should == 1234

      CLI.parse_args(%w(-p 1234))
      CLI.config[:port].should == 1234
    end

    it "kafka topic" do
      CLI.parse_args(%w(--topic news))
      CLI.config[:topic].should == "news"

      CLI.parse_args(%w(-t news))
      CLI.config[:topic].should == "news"
    end

    it "kafka compression" do
      CLI.stub(:publish? => true)

      CLI.parse_args(%w(--compression no))
      CLI.config[:compression].should == Message::NO_COMPRESSION
      CLI.parse_args(%w(-c no))
      CLI.config[:compression].should == Message::NO_COMPRESSION

      CLI.parse_args(%w(--compression gzip))
      CLI.config[:compression].should == Message::GZIP_COMPRESSION
      CLI.parse_args(%w(-c gzip))
      CLI.config[:compression].should == Message::GZIP_COMPRESSION

      CLI.parse_args(%w(--compression snappy))
      CLI.config[:compression].should == Message::SNAPPY_COMPRESSION
      CLI.parse_args(%w(-c snappy))
      CLI.config[:compression].should == Message::SNAPPY_COMPRESSION
    end

    it "message" do
      CLI.stub(:publish? => true)
      CLI.parse_args(%w(--message YEAH))
      CLI.config[:message].should == "YEAH"

      CLI.parse_args(%w(-m YEAH))
      CLI.config[:message].should == "YEAH"
    end

  end

  describe "config validation" do
    it "should assign a default port" do
      CLI.stub(:exit)
      CLI.stub(:puts)
      CLI.validate_config
      CLI.config[:port].should == Kafka::IO::PORT
    end
  end

  it "should assign a default host" do
    CLI.stub(:exit)
    CLI.validate_config
    CLI.config[:host].should == Kafka::IO::HOST
  end


  it "read compression method" do
    CLI.string_to_compression("no").should == Message::NO_COMPRESSION
    CLI.string_to_compression("gzip").should == Message::GZIP_COMPRESSION
    CLI.string_to_compression("snappy").should == Message::SNAPPY_COMPRESSION
    lambda { CLI.push(:string_to_compression,nil) }.should raise_error
  end

end
