require 'set'
require 'json'

module Kvstore

  # Each entry in the data file is structured as follows:
  # 32-bit key length
  # 32-bit data length
  # 32-bit data block reserved length
  # (key length)-bytes of key string
  # (data length)-bytes of data
  # (reserved length - data length)-bytes of null padding
  
  class BasicStore
    class NoObject; end

    def initialize(filename, index_filename)
      @kv_memcache = Hash.new
      @dirty_keys = Set.new

      # Each entry in @index is an array as follows:
      # @index[n][0] == the offset where the block begins in the data
      # @index[n][1] == the offset where the block begins in the index
      # @index[n][2] == the reserved block size
      @index = Hash.new

      @min_block_size = 1024

      open_files(filename, index_filename)
    end

    def put(key, value)
      @dirty_keys << key
      @kv_memcache[key] = value
    end

    def get(key)
      value = @kv_memcache.fetch(key, NoObject)
      if value == NoObject
        raise NoKeyError unless @index.key?(key)
        value = read_key_from_file(key)
        @kv_memcache[key] = value
      end
      value
    end

    def flush
      flush_dirty
    end

    def close
      flush_dirty
      @data_f.close
      @index_f.close
      @data_f = nil
      @index_f = nil
    end

    private

    def serialize_value(value)
      JSON.dump(value).b
    end

    def unserialize_value(bin_val)
      JSON.load(bin_val.encode('utf-8'))
    end

    def read_key_from_file(key)
      position = @index[key][0]
      @data_f.seek(position, :SET)
      key_len, bin_val_len = @data_f.read(8).unpack('L<L<')
      @data_f.seek(4 + key_len, :CUR)
      bin_val = @data_f.read(bin_val_len)
      unserialize_value(bin_val)
    end

    def flush_dirty
      @dirty_keys.each do |key|
        key_index = @index.fetch(key, nil)
        if key_index == nil
          flush_add_entry(key)
        else
          flush_existing_entry(key)
        end
      end
      @dirty_keys.clear

      @index_f.seek(0, :SET)
      @index_f.write([ @index.length ].pack('L<'))
      @data_f.seek(0, :SET)
      @data_f.write([ @index.length ].pack('L<'))
    end

    def flush_existing_entry(key)
      val = serialize_value(@kv_memcache[key])
      val_len = val.length
      key_bin = key.b
      key_len = key_bin.length

      key_index = @index[key] 
      just_write = proc do
        @data_f.seek(key_index[0] + 4, :SET)
        @data_f.write([ val_len ].pack('L<'))
        @data_f.seek(key_len + 4, :CUR)
        @data_f.write(val)
      end

      if val_len <= @min_block_size
        just_write[]
      else
        if val_len <= key_index[2]
          just_write[]
        else
          @data_f.seek(key_index[0], :SET)
          @data_f.write("\0" * 8)
          @data_f.seek(0, :END)
          @index[key] = [@data_f.tell, key_index[1], val_len]
          write_full_entry(key, val)
          @index_f.seek(key_index[1], :SET)
          write_entry_index(key)
        end
      end
    end

    def flush_add_entry(key)
      val = serialize_value(@kv_memcache[key])
      @data_f.seek(0, :END)
      @index_f.seek(0, :END)
      @index[key] = [@data_f.tell, @index_f.tell, [val.length, @min_block_size].max]
      write_full_entry(key, val)
      write_entry_index(key)
    end

    def write_entry_index(key)
      index_key = @index[key]
      key_bin = key.b
      key_len = key_bin.length
      @index_f.write([ key_len ].pack('L<'))
      @index_f.write(key_bin)
      @index_f.write([ index_key[0], index_key[2] ].pack('Q<L<'))
    end

    def write_full_entry(key, val_bin)
      block_size = @index[key][2]
      val_len = val_bin.length
      key_bin = key.b
      key_len = key_bin.length
      @data_f.write([ key_len, val_len, block_size ].pack('L<L<L<'))
      @data_f.write(key_bin)
      @data_f.write(val_bin)
      if val_len < block_size
        @data_f.write("\0" * (block_size - val_len))
      end
    end

    def open_files(data_name, index_name)
      if !try_open_files(data_name, index_name)
        create_initial_files(data_name, index_name)
        try_open_files(data_name, index_name)
      else
        load_all_indices
      end
    end

    def load_all_indices
      @index.clear
      @index_f.seek(0, :SET)
      num_indices = @index_f.read(4).unpack('L<')[0]
      num_indices.times do
        index_offset = @index_f.tell
        key_len = @index_f.read(4).unpack('L<')[0]
        key = @index_f.read(key_len).encode('utf-8')
        position, block_size = @index_f.read(12).unpack('Q<L<')
        @index[key] = [position, index_offset, block_size]
      end
    end

    def try_open_files(data_name, index_name)
      begin
        @data_f = File.open(data_name, 'r+b', autoclose: false)
        @index_f = File.open(index_name, 'r+b', autoclose: false)
      rescue Errno::ENOENT
        if @data_f 
          @data_f.close
          @data_f = nil
        end
        false
      else
        true
      end
    end

    def create_initial_files(data_name, index_name)
      data_f = File.open(data_name, 'wb')
      data_f.write([ 0 ].pack('L<'))
      data_f.close

      index_f = File.open(index_name, 'wb')
      index_f.write([ 0 ].pack('L<'))
      index_f.close
    end
  end
end

