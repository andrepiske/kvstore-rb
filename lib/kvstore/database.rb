# This file is part of Kvstore
# (2016) André Diego Piske
# https://github.com/andrepiske/kvstore-rb
# See LICENSE file for the license
#
module Kvstore
  class Database
    attr_reader :filename
    attr_reader :index_filename

    def initialize(filename)
      @filename = filename
      @index_filename = "#{filename}.idx"

      @engine = Kvstore::BasicStore.new(@filename, @index_filename)
    end

    def []=(key, value)
      @engine.put(key, value)
    end

    def [](key)
      @engine.get(key)
    end

    def fetch(key, default_value=nil)
      self[key]
    rescue ::Kvstore::NoKeyError
      default_value
    end

    def flush
      @engine.flush
    end

    def close
      @engine.close
    end
  end
end
