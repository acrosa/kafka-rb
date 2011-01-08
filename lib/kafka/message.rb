module Kafka

  # A message. The format of an N byte message is the following:
  # 1 byte "magic" identifier to allow format changes
  # 4 byte CRC32 of the payload
  # N - 5 byte payload
  class Message
    MAGIC_IDENTIFIER_DEFAULT = 0
    attr_accessor :magic, :checksum, :payload

    def initialize(payload = nil, magic = MAGIC_IDENTIFIER_DEFAULT)
      self.magic = magic
      self.payload = payload
      self.checksum = self.calculate_checksum
    end

    def calculate_checksum
      Zlib.crc32(self.payload)
    end

    def valid?
      self.checksum == Zlib.crc32(self.payload)
    end
  end
end