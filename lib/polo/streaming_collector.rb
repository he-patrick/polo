module Polo
  class StreamingCollector
    BATCH_SIZE = 1000

    def initialize(base_class, id, dependency_tree, seen_records)
      @base_class = base_class
      @id = id
      @dependency_tree = normalize_dependency_tree(dependency_tree)
      @seen_records = seen_records
    end

    # Public: Collects records from the database and yields RECORDS
    #
    # batch_size - The number of records to collect per batch (default: 1000).
    # block - A block that will be yielded with an array of ActiveRecord objects for each batch.
    #
    def collect_stream(batch_size: BATCH_SIZE, &block)
      record = @base_class.find_by(@base_class.primary_key => @id)
      return unless record

      yield [record]

      traverse_associations(record, @dependency_tree, batch_size, &block)
    end

    private

    def normalize_dependency_tree(deps)
      case deps
      when Symbol, String
        { deps.to_sym => {} }
      when Array
        deps.each_with_object({}) do |dep, hash|
          hash.merge!(normalize_dependency_tree(dep))
        end
      when Hash
        deps.each_with_object({}) do |(key, value), hash|
          hash[key.to_sym] = normalize_dependency_tree(value)
        end
      else
        {}
      end
    end

    def traverse_associations(record, dependencies, batch_size, &block)
      dependencies.each do |assoc_name, nested_deps|
        reflection = record.class.reflect_on_association(assoc_name)
        next unless reflection

        case reflection.macro
        when :belongs_to
          traverse_belongs_to(record, reflection, nested_deps, batch_size, &block)
        when :has_one
          if reflection.through_reflection
            traverse_has_one_through(record, reflection, nested_deps, batch_size, &block)
          else
            traverse_has_one(record, reflection, nested_deps, batch_size, &block)
          end
        when :has_many
          if reflection.through_reflection
            traverse_has_many_through(record, reflection, nested_deps, batch_size, &block)
          else
            traverse_has_many(record, reflection, nested_deps, batch_size, &block)
          end
        when :has_and_belongs_to_many
          traverse_habtm(record, reflection, nested_deps, batch_size, &block)
        end
      end
    end

    def traverse_belongs_to(record, reflection, nested_deps, batch_size, &block)
      associated = record.send(reflection.name)
      return unless associated
      return if seen?(associated)

      mark_seen(associated)
      yield [associated]

      traverse_associations(associated, nested_deps, batch_size, &block) if nested_deps.any?
    end

    def traverse_has_one_through(record, reflection, nested_deps, batch_size, &block)
      through_reflection = reflection.through_reflection
      source_reflection = reflection.source_reflection
      return unless through_reflection && source_reflection
    
      through_record = record.send(through_reflection.name)
      return unless through_record
    
      unless seen?(through_record)
        mark_seen(through_record)
        yield [through_record]
      end
    
      source_record = through_record.send(source_reflection.name)
      return unless source_record
    
      unless seen?(source_record)
        mark_seen(source_record)
        yield [source_record]
    
        traverse_associations(source_record, nested_deps, batch_size, &block) if nested_deps.any?
      end
    end

    def traverse_has_one(record, reflection, nested_deps, batch_size, &block)
      associated = record.send(reflection.name)
      return unless associated
      return if seen?(associated)

      mark_seen(associated)
      yield [associated]

      traverse_associations(associated, nested_deps, batch_size, &block) if nested_deps.any?
    end

    def traverse_has_many(record, reflection, nested_deps, batch_size, &block)
      record.send(reflection.name).find_in_batches(batch_size: batch_size) do |batch|
        new_records = batch.reject { |r| seen?(r) }
        new_records.each { |r| mark_seen(r) }

        yield new_records if new_records.any?

        if nested_deps.any?
          new_records.each do |assoc_record|
            traverse_associations(assoc_record, nested_deps, batch_size, &block)
          end
        end
      end
    end

    def traverse_has_many_through(record, reflection, nested_deps, batch_size, &block)
      through_reflection = reflection.through_reflection
      source_reflection = reflection.source_reflection
      return unless through_reflection && source_reflection

      record.send(through_reflection.name).find_in_batches(batch_size: batch_size) do |through_batch|
        new_through_records = through_batch.reject { |r| seen?(r) }
        new_through_records.each { |r| mark_seen(r) }

        yield new_through_records if new_through_records.any?

        source_ids = through_batch.map { |tr| tr.send(source_reflection.foreign_key) }.compact.uniq
        source_class = source_reflection.klass
        source_records = source_class.where(source_class.primary_key => source_ids).to_a

        new_source_records = source_records.reject { |r| seen?(r) }
        new_source_records.each { |r| mark_seen(r) }

        yield new_source_records if new_source_records.any?

        if nested_deps.any?
          new_source_records.each do |source_record|
            traverse_associations(source_record, nested_deps, batch_size, &block)
          end
        end
      end
    end

    def traverse_habtm(record, reflection, nested_deps, batch_size, &block)
      join_table = reflection.join_table
      foreign_key = reflection.foreign_key
      association_foreign_key = reflection.association_foreign_key
      record_id = record.send(record.class.primary_key)

      record.send(reflection.name).find_in_batches(batch_size: batch_size) do |batch|
        new_records = batch.reject { |r| seen?(r) }
        new_records.each { |r| mark_seen(r) }

        yield new_records if new_records.any?

        join_records = batch.map do |assoc_record|
          assoc_id = assoc_record.send(assoc_record.class.primary_key)
          join_key = "#{record_id}_#{assoc_id}_#{join_table}"

          next if @seen_records[join_key]
          @seen_records[join_key] = true

          {
            table_name: join_table,
            values: {
              foreign_key => record_id,
              association_foreign_key => assoc_id
            }
          }
        end.compact

        yield join_records if join_records.any?

        if nested_deps.any?
          new_records.each do |assoc_record|
            traverse_associations(assoc_record, nested_deps, batch_size, &block)
          end
        end
      end
    end

    def record_key(record)
      pk = record.class.primary_key
      "#{record.class.name}_#{record.send(pk)}"
    end

    def seen?(record)
      @seen_records[record_key(record)]
    end

    def mark_seen(record)
      @seen_records[record_key(record)] = true
    end
  end
end
