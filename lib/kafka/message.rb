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
    COMPRESSION_CODEC_MASK = 0x03

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

    # Takes a byte string containing one or more messages; returns a MessageSet
    # with the messages parsed from the string, and the number of bytes
    # consumed from the string.
    def self.parse_from(data)
      messages = []
      bytes_processed = 0

      while bytes_processed <= data.length - 5 # 5 = size of BASIC_MESSAGE_HEADER
        message_size, magic = data[bytes_processed, 5].unpack(BASIC_MESSAGE_HEADER)
        break if bytes_processed + message_size + 4 > data.length # message is truncated

        case magic
        when 0
          # |  0  |  1  |  2  |  3  |  4  |  5  |  6  |  7  |  8  |  9      ...
          # |                       |     |                       |
          # |      message_size     |magic|        checksum       | payload ...
          payload_size = message_size - 5 # 5 = sizeof(magic) + sizeof(checksum)
          checksum = data[bytes_processed + 5, 4].unpack(VERSION_0_HEADER).shift
          payload  = data[bytes_processed + 9, payload_size]
          messages << Kafka::Message.new(payload, magic, checksum)

        when 1
          # |  0  |  1  |  2  |  3  |  4  |  5  |  6  |  7  |  8  |  9  | 10      ...
          # |                       |     |     |                       |
          # |         size          |magic|attrs|        checksum       | payload ...
          payload_size = message_size - 6 # 6 = sizeof(magic) + sizeof(attrs) + sizeof(checksum)
          attributes, checksum = data[bytes_processed + 5, 5].unpack(VERSION_1_HEADER)
          payload = data[bytes_processed + 10, payload_size]

          case attributes & COMPRESSION_CODEC_MASK
          when 0 # a single uncompressed message
            messages << Kafka::Message.new(payload, magic, checksum)
          when 1 # a gzip-compressed message set -- parse recursively
            uncompressed = Zlib::GzipReader.new(StringIO.new(payload)).read
            message_set = parse_from(uncompressed)
            raise 'malformed compressed message' if message_set.size != uncompressed.size
            messages.concat(message_set.messages)
          else
            # https://cwiki.apache.org/confluence/display/KAFKA/Compression
            # claims that 2 is for Snappy compression, but Kafka's Scala client
            # implementation doesn't seem to support it yet, so I don't have
            # a reference implementation to test against.
            raise "Unsupported Kafka compression codec: #{attributes & COMPRESSION_CODEC_MASK}"
          end

        else
          raise "Unsupported Kafka message version: magic number #{magic}"
        end

        bytes_processed += message_size + 4 # 4 = sizeof(message_size)
      end

      MessageSet.new(bytes_processed, messages)
    end
  end

  # Encapsulates a list of Kafka messages (as Kafka::Message objects in the
  # +messages+ attribute) and their total serialized size in bytes (the +size+
  # attribute).
  class MessageSet < Struct.new(:size, :messages); end
end
