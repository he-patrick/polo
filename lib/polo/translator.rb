require "polo/sql_translator"
require "polo/configuration"
require "polo/obfuscator"

module Polo
  class Translator
    include Obfuscator

    # Public: Creates a new Polo::Collector
    #
    # selects - An array of SELECT queries
    #
    def initialize(selects, configuration=Configuration.new)
      @selects = selects
      @configuration = configuration
    end

    # Public: Translates SELECT queries into INSERTS.
    #
    def translate
      SqlTranslator.new(instances, @configuration).to_sql.uniq
    end

    def instances
      active_record_selects, raw_selects = @selects.partition{|s| s[:klass]}

      active_record_instances = active_record_selects.flat_map do |select|
        select[:klass].find_by_sql(select[:sql]).to_a
      end

      if fields = @configuration.blacklist
        obfuscate!(active_record_instances, fields)
      end

      raw_instance_values = raw_selects.flat_map do |select|
        table_name = select[:sql][/^SELECT .* FROM (?:"|`)([^"`]+)(?:"|`)/, 1]
        select[:connection].select_all(select[:sql]).map { |values| {table_name: table_name, values: values} }
      end

      active_record_instances + raw_instance_values
    end
  end
end
