require 'set'
require 'json'

module Kvstore
  class BasicStore
    class NoObject; end

    def initialize(filename, index_filename)
      @kv_memcache = Hash.new
      @dirty_keys = Set.new
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
      position = @index[key]
      @data_f.seek(position, :SET)
      key_len = @data_f.read(4).unpack('L<')[0]
      @data_f.seek(key_len, :CUR)
      bin_val_len = @data_f.read(4).unpack('L<')[0]
      bin_val = @data_f.read(bin_val_len)
      unserialize_value(bin_val)
    end

    def flush_dirty
      num_added_entries = 0
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

      just_write = proc do
        @data_f.seek(@index[key] + key_len + 4, :SET)
        @data_f.write([ val_len ].pack('L<'))
        @data_f.write(val)
      end

      if val_len <= @min_block_size
        just_write[]
      else
        @data_f.seek(@index[key] + key_len + 4, :SET)
        current_val_len = @data_f.read(4).unpack('<L')[0]
        if val_len <= current_val_len
          just_write[]
        else
          @data_f.seek(@index[key], :SET)
          @data_f.write("\0" * 4)
          flush_add_entry(key)
        end
      end
    end

    def flush_add_entry(key)
      @data_f.seek(0, :END)
      @index[key] = @data_f.tell
      write_full_entry(key, serialize_value(@kv_memcache[key]))
      @index_f.seek(0, :END)
      write_entry_index(key)
    end

    def write_entry_index(key)
      offset = @index[key]
      key_bin = key.b
      key_len = key_bin.length
      @index_f.write([ key_len ].pack('L<'))
      @index_f.write(key_bin)
      @index_f.write([ offset ].pack('Q<'))
    end

    def write_full_entry(key, val_bin)
      val_len = val_bin.length
      key_bin = key.b
      key_len = key_bin.length
      @data_f.write([ key_len ].pack('L<'))
      @data_f.write(key_bin)
      @data_f.write([ val_len ].pack('L<'))
      @data_f.write(val_bin)
      if val_len < @min_block_size
        @data_f.write("\0" * (@min_block_size - val_len))
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
        key_len = @index_f.read(4).unpack('L<')[0]
        key = @index_f.read(key_len).encode('utf-8')
        position = @index_f.read(8).unpack('Q<')[0]
        @index[key] = position
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

