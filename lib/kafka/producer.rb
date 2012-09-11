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
  class Producer

    include Kafka::IO

    attr_accessor :topic, :partition

    def initialize(options = {})
      self.topic     = options[:topic]      || "test"
      self.partition = options[:partition]  || 0
      self.host      = options[:host]       || HOST
      self.port      = options[:port]       || PORT
      self.connect(self.host, self.port)
    end

    def send(messages)
      self.write(Encoder.produce(self.topic, self.partition, messages))
    end

    def batch(&block)
      batch = Kafka::Batch.new
      block.call( batch )
      self.send(batch.messages)
      batch.messages.clear
    end
  end
end
