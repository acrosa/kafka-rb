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

  # A message. The format of a message is as follows:
  #
  # 4 byte big-endian int: length of message in bytes (including the rest of
  #                        the header, but excluding the length field itself)
  # 1 byte: "magic" identifier (format version number)
  #
  # If the magic byte == 0, there is one more header field:
  #
  # 4 byte big-endian int: CRC32 checksum of the payload
  #
  # If the magic byte == 1, there are two more header fields:
  #
  # 1 byte: "attributes" (flags for compression, codec etc)
  # 4 byte big-endian int: CRC32 checksum of the payload
  #
  # All following bytes are the payload.
  class Message

    MAGIC_IDENTIFIER_DEFAULT = 0
    BASIC_MESSAGE_HEADER = 'NC'.freeze
    VERSION_0_HEADER = 'N'.freeze
    VERSION_1_HEADER = 'CN'.freeze

    attr_accessor :magic, :checksum, :payload

    def initialize(payload = nil, magic = MAGIC_IDENTIFIER_DEFAULT, checksum = nil)
      self.magic    = magic
      self.payload  = payload
      self.checksum = checksum || self.calculate_checksum
    end

    def calculate_checksum
      Zlib.crc32(self.payload)
    end

    def valid?
      self.checksum == calculate_checksum
    end

    def self.parse_from(binary)
      size, magic = binary.unpack(BASIC_MESSAGE_HEADER)
      case magic
      when 0
        checksum = binary[5, 4].unpack(VERSION_0_HEADER).shift # 5 = sizeof(length) + sizeof(magic)
        payload = binary[9, size] # 9 = sizeof(length) + sizeof(magic) + sizeof(checksum)
        Kafka::Message.new(payload, magic, checksum)

      when 1
        attributes, checksum = binary[5, 5].unpack(VERSION_1_HEADER)
        payload = binary[10, size] # 10 = sizeof(length) + sizeof(magic) + sizeof(attrs) + sizeof(checksum)
        # TODO interpret attributes
        Kafka::Message.new(payload, magic, checksum)

      else
        raise "Unsupported Kafka message version: magic number #{magic}"
      end
    end
  end
end
