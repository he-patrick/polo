module Polo
  module Obfuscator
    private

    def obfuscate!(instances, fields)
      instances.each do |instance|
        next if intersection(instance.attributes.keys, fields).empty?

        fields.each do |field, strategy|
          field = field.to_s

          if table = table_name(field)
            field = field_name(field)
          end

          correct_table = table.nil? || instance.class.table_name == table

          if correct_table && instance.attributes[field]
            instance.send("#{field}=", new_field_value(field, strategy, instance))
          end
        end
      end
    end

    def field_name(field)
      field.to_s.include?('.') ? field.split('.').last : field.to_s
    end

    def table_name(field)
      field.to_s.include?('.') ? field.split('.').first : nil
    end

    def intersection(attrs, fields)
      attrs & fields.map { |pair| field_name(pair.first) }
    end

    def new_field_value(field, strategy, instance)
      value = instance.attributes[field]
      if strategy.nil?
        value.split("").shuffle.join
      else
        strategy.arity == 1 ? strategy.call(value) : strategy.call(value, instance)
      end
    end
  end
end