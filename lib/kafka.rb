require 'socket'
require 'zlib'
if RUBY_VERSION[0,3] == "1.8"
  require 'iconv'
end

require File.join(File.dirname(__FILE__), "kafka", "io")
require File.join(File.dirname(__FILE__), "kafka", "request_type")
require File.join(File.dirname(__FILE__), "kafka", "batch")
require File.join(File.dirname(__FILE__), "kafka", "message")
require File.join(File.dirname(__FILE__), "kafka", "producer")
require File.join(File.dirname(__FILE__), "kafka", "consumer")

module Kafka
end
