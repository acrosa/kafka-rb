$KCODE = 'UTF-8'

require 'zlib'

PRODUCE_REQUEST_ID = 0

def encode_message(message)
    # <MAGIC_BYTE: char> <CRC32: int> <PAYLOAD: bytes>
    data = [0].pack("C").to_s + [Zlib.crc32(message)].pack('N').to_s + message
    # print ("CHECKSUM " + Zlib.crc32(message).to_s)
    # print ("MES " + data.length.to_s)
    return data
end
# encode_message("ale")

def encode_produce_request(topic, partition, messages)
    encoded = messages.collect { |m| encode_message(m) }
    message_set = encoded.collect { |e| puts "Message size #{e.length}"; [e.length].pack("N") + e }.join("")

    puts "MESSAGE"
    puts message_set.inspect

    data = [PRODUCE_REQUEST_ID].pack("n") + \
           [topic.length].pack("n") + topic.to_s + \
           [partition].pack("N") + \
           [message_set.length].pack("N") + message_set
    puts "DATA " + message_set.length.to_s
    return [data.length].pack("N") + data
end

socket = TCPSocket.open("localhost", 9092)
socket.write encode_produce_request("test", 0, ["ale"])
