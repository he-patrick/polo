require "polo/sql_translator"
require "polo/configuration"
require "polo/obfuscator"

module Polo
  class StreamingTranslator
    include Obfuscator

    # Public: Creates a new StreamingTranslator
    #
    # records - An array of ActiveRecord objects
    # configuration - Polo configuration
    #
    def initialize(records, configuration = Configuration.new)
      @records = records
      @configuration = configuration
    end

    # Public: Translates records into INSERT statements.
    #
    # Only ActiveRecord objects are obfuscated.
    #
    def translate
      return [] if @records.empty?

      if fields = @configuration.blacklist
        ar_records = @records.select { |r| r.respond_to?(:attributes) }
        obfuscate!(ar_records, fields)
      end

      SqlTranslator.new(@records, @configuration).to_sql
    end
  end
end