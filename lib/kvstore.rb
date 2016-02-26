# This file is part of Kvstore
# (2016) André Diego Piske
# https://github.com/andrepiske/kvstore-rb
# See LICENSE file for the license
#
module Kvstore
  class NoKeyError < StandardError; end
end

require 'kvstore/store'
require 'kvstore/database'
