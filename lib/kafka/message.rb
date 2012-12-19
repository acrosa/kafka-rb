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
    MAGIC_IDENTIFIER_COMPRESSION = 1
    NO_COMPRESSION = 0
    GZIP_COMPRESSION = 1
    SNAPPY_COMPRESSION = 2
    BASIC_MESSAGE_HEADER = 'NC'.freeze
    VERSION_0_HEADER = 'N'.freeze
    VERSION_1_HEADER = 'CN'.freeze
    COMPRESSION_CODEC_MASK = 0x03

    attr_accessor :magic, :checksum, :payload

    def initialize(payload = nil, magic = MAGIC_IDENTIFIER_DEFAULT, checksum = nil)
      self.magic       = magic
      self.payload     = payload || ""
      self.checksum    = checksum || self.calculate_checksum
      @compression = NO_COMPRESSION
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
        when MAGIC_IDENTIFIER_DEFAULT
          # |  0  |  1  |  2  |  3  |  4  |  5  |  6  |  7  |  8  |  9      ...
          # |                       |     |                       |
          # |      message_size     |magic|        checksum       | payload ...
          payload_size = message_size - 5 # 5 = sizeof(magic) + sizeof(checksum)
          checksum = data[bytes_processed + 5, 4].unpack(VERSION_0_HEADER).shift
          payload  = data[bytes_processed + 9, payload_size]
          messages << Kafka::Message.new(payload, magic, checksum)

        when MAGIC_IDENTIFIER_COMPRESSION
          # |  0  |  1  |  2  |  3  |  4  |  5  |  6  |  7  |  8  |  9  | 10      ...
          # |                       |     |     |                       |
          # |         size          |magic|attrs|        checksum       | payload ...
          payload_size = message_size - 6 # 6 = sizeof(magic) + sizeof(attrs) + sizeof(checksum)
          attributes, checksum = data[bytes_processed + 5, 5].unpack(VERSION_1_HEADER)
          payload = data[bytes_processed + 10, payload_size]

          case attributes & COMPRESSION_CODEC_MASK
          when NO_COMPRESSION # a single uncompressed message
            messages << Kafka::Message.new(payload, magic, checksum)
          when GZIP_COMPRESSION # a gzip-compressed message set -- parse recursively
            uncompressed = Zlib::GzipReader.new(StringIO.new(payload)).read
            message_set = parse_from(uncompressed)
            raise 'malformed compressed message' if message_set.size != uncompressed.size
            messages.concat(message_set.messages)
          when SNAPPY_COMPRESSION # a snappy-compresses message set -- parse recursively
            ensure_snappy! do
              uncompressed = Snappy::Reader.new(StringIO.new(payload)).read
              message_set = parse_from(uncompressed)
              raise 'malformed compressed message' if message_set.size != uncompressed.size
              messages.concat(message_set.messages)
            end
          else
            # https://cwiki.apache.org/confluence/display/KAFKA/Compression
            raise "Unsupported Kafka compression codec: #{attributes & COMPRESSION_CODEC_MASK}"
          end

        else
          raise "Unsupported Kafka message version: magic number #{magic}"
        end

        bytes_processed += message_size + 4 # 4 = sizeof(message_size)
      end

      MessageSet.new(bytes_processed, messages)
    end

    def encode(compression = NO_COMPRESSION)
      @compression = compression

      self.payload = asciify_payload
      self.payload = compress_payload if compression?

      data = magic_and_compression + [calculate_checksum].pack("N") + payload
      [data.length].pack("N") + data
    end


    # Encapsulates a list of Kafka messages (as Kafka::Message objects in the
    # +messages+ attribute) and their total serialized size in bytes (the +size+
    # attribute).
    class MessageSet < Struct.new(:size, :messages); end

    def self.ensure_snappy!
      if Object.const_defined? "Snappy"
        yield
      else
        fail "Snappy not available!"
      end
    end

    def ensure_snappy! &block
      self.class.ensure_snappy! &block
    end

    private

    attr_reader :compression

    def compression?
      compression != NO_COMPRESSION
    end

    def magic_and_compression
      if compression?
        [MAGIC_IDENTIFIER_COMPRESSION, compression].pack("CC")
      else
        [MAGIC_IDENTIFIER_DEFAULT].pack("C")
      end
    end

    def asciify_payload
      if RUBY_VERSION[0, 3] == "1.8"
        payload
      else
        payload.to_s.force_encoding(Encoding::ASCII_8BIT)
      end
    end

    def compress_payload
      case compression
        when GZIP_COMPRESSION
          gzip
        when SNAPPY_COMPRESSION
          snappy
      end
    end

    def gzip
      with_buffer do |buffer|
        gz = Zlib::GzipWriter.new buffer, nil, nil
        gz.write payload
        gz.close
      end
    end

    def snappy
      ensure_snappy! do
        with_buffer do |buffer|
          Snappy::Writer.new buffer do |w|
            w << payload
          end
        end
      end
    end

    def with_buffer
      buffer = StringIO.new
      buffer.set_encoding Encoding::ASCII_8BIT unless RUBY_VERSION =~ /^1\.8/
      yield buffer if block_given?
      buffer.rewind
      buffer.string
    end
  end
end

