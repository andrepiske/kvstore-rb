require 'minitest/autorun'
require 'kvstore'
require 'tempfile'
require 'set'

describe Kvstore::Database do
  let :pre_filled_db_path do
    file_name = temp_file_path
    db = Kvstore::Database.new(file_name)
    db['some_key'] = { number: 42, 'some_hash' => { hey: 'ho!', lets: 'go' } }
    db['another_one'] = [ 'is an array', 'or whatever', 1337 ]
    db['some string value'] = 'Oi, I\'m a String! Look At Me!'
    db.close
    file_name
  end

  describe 'pre_filled_db_path' do
    it 'has all expected keys and values' do
      file_name = pre_filled_db_path
      db = Kvstore::Database.new(file_name)
      refute_nil db

      some_key_values = db['some_key']
      assert some_key_values.is_a?(Hash)
      assert some_key_values.key?('number')
      assert_equal 42, some_key_values['number']

      assert_equal({ 'hey' => 'ho!', 'lets' => 'go' }, some_key_values['some_hash'])

      another_one_value = db['another_one']
      assert another_one_value.is_a?(Array)
      assert_equal [ 'is an array', 'or whatever', 1337 ], another_one_value

      assert db['some string value'].is_a?(String)
      assert_equal 'Oi, I\'m a String! Look At Me!', db['some string value']
    end
  end

  describe 'fetching an unexistant key' do
    it 'raises an error' do
      file_name = pre_filled_db_path
      db = Kvstore::Database.new(file_name)

      refute_nil db
      assert_raises 'Kvstore::NoKeyError' do
        db['qux']
      end
    end

    it 'returns default value when using #fetch' do
      file_name = pre_filled_db_path
      db = Kvstore::Database.new(file_name)

      refute_nil db
      assert_raises 'Kvstore::NoKeyError' do
        db['xpto']
      end
      assert_equal 42, db.fetch('xpto', 42)
    end
  end

  describe 'overwriting smaller keys' do
    it 'writes and reads the new shorter key' do
      file_name = pre_filled_db_path
      db = Kvstore::Database.new(file_name)

      assert db['another_one'].is_a?(Array)
      db['another_one'] = 900
      assert_equal 900, db['another_one']

      db.close
      db = Kvstore::Database.new(file_name)
      assert_equal 900, db['another_one']
    end
  end

  describe 'overwriting longer keys' do
    it 'writes and reads the new longer key' do
      file_name = pre_filled_db_path
      db = Kvstore::Database.new(file_name)

      # 2 megabytes value
      alphabet = ('a'..'z').to_a
      very_long_value = (1..(1024 * 1024 * 2)).map{ alphabet[rand 26] }.join
      assert very_long_value.is_a?(String)
      assert_equal (1024 * 1024 * 2), very_long_value.length

      assert db['another_one'].is_a?(Array)
      db['another_one'] = very_long_value
      assert_equal very_long_value, db['another_one']

      db.close
      db = Kvstore::Database.new(file_name)
      assert_equal very_long_value, db['another_one']
    end
  end

  def teardown
    @temp_file_names.each do |temp_path|
      File.unlink(temp_path) rescue nil
    end
  end

  def setup
    @temp_file_names = Set.new
  end

  private

  def temp_file_path
    temp_file = Tempfile.new('kvstore_test.db')
    temp_file.close
    path = temp_file.path
    idx_path = "#{path}.idx"

    File.unlink(path) rescue nil
    File.unlink(idx_path) rescue nil

    @temp_file_names << path
    @temp_file_names << idx_path
    path
  end
end

