require "polo/version"
require "polo/collector"
require "polo/translator"
require "polo/configuration"
require "polo/streaming_collector"
require "polo/streaming_translator"
require "initializers/arel"
require "initializers/big_decimal"

module Polo

  # Public: Traverses a dependency graph based on a seed ActiveRecord object
  # and generates all the necessary INSERT queries for each one of the records
  # it finds along the way.
  #
  # base_class - An ActiveRecord::Base class for the seed record.
  # id - An ID used to find the desired seed record.
  #
  # dependency_tree - An ActiveRecord::Associations::Preloader compliant that
  # will define the path Polo will traverse.
  #
  # (from ActiveRecord::Associations::Preloader docs)
  # It may be:
  # - a Symbol or a String which specifies a single association name. For
  #   example, specifying +:books+ allows this method to preload all books
  #   for an Author.
  # - an Array which specifies multiple association names. This array
  #   is processed recursively. For example, specifying <tt>[:avatar, :books]</tt>
  #   allows this method to preload an author's avatar as well as all of his
  #   books.
  # - a Hash which specifies multiple association names, as well as
  #   association names for the to-be-preloaded association objects. For
  #   example, specifying <tt>{ author: :avatar }</tt> will preload a
  #   book's author, as well as that author's avatar.
  #
  # +:associations+ has the same format as the +:include+ option for
  # <tt>ActiveRecord::Base.find</tt>. So +associations+ could look like this:
  #
  #   :books
  #   [ :books, :author ]
  #   { author: :avatar }
  #   [ :books, { author: :avatar } ]
  #
  def self.explore(base_class, id, dependencies={})
    Traveler.collect(base_class, id, dependencies).translate(defaults)
  end

  # Public: Streaming version of explore that yields SQL statements in batches.
  # Uses find_in_batches to process records AND associations in memory-efficient chunks.
  #
  # This method streams both:
  # 1. Base records in batches (when multiple IDs provided)
  # 2. Associations for each record in batches (handles large association trees)
  #
  # base_class - An ActiveRecord::Base class for the seed record.
  # id - An ID or array of IDs to find records (optional, nil streams all records).
  # dependencies - Association dependency tree (same format as explore).
  # batch_size - Number of records to process per batch (default: 1000).
  #
  # Yields arrays of INSERT SQL statements for each batch.
  #
  # Example:
  #   Polo.explore_stream(User, user_ids, :posts, batch_size: 500) do |statements|
  #     statements.each { |sql| file.puts(sql) }
  #   end
  #
  def self.explore_stream(base_class, id, dependencies = {}, batch_size: 1000, &block)
    StreamingTraveler.collect_stream(base_class, id, dependencies, defaults, batch_size, &block)
  end

  # Public: Sets up global settings for Polo
  #
  # block - Takes a block with the settings you decide to use
  #
  #   obfuscate - Takes a blacklist with sensitive fields you wish to scramble
  #   on_duplicate - Defines the on_duplicate strategy for your INSERTS
  #     e.g. :override, :ignore
  #
  # usage:
  #   Polo.configure do
  #     obfuscate(:email, :password, :credit_card)
  #     on_duplicate(:override)
  #   end
  #
  def self.configure(&block)
    @configuration = Configuration.new
    @configuration.instance_eval(&block) if block_given?
    @configuration
  end

  # Public: Returns the default settings
  #
  def self.defaults
    @configuration || configure
  end


  class Traveler

    def self.collect(base_class, id, dependencies={})
      selects = Collector.new(base_class, id, dependencies).collect
      new(selects)
    end

    def initialize(selects)
      @selects = selects
    end

    def translate(configuration=Configuration.new)
      Translator.new(@selects, configuration).translate
    end
  end

  class StreamingTraveler
    def self.collect_stream(base_class, id, dependencies, configuration, batch_size, &block)
      new(base_class, id, dependencies, configuration, batch_size).stream(&block)
    end

    def initialize(base_class, id, dependencies, configuration, batch_size)
      @base_class = base_class
      @id = id
      @dependencies = dependencies
      @configuration = configuration
      @batch_size = batch_size
      @seen_records = {}
    end

    def stream(&block)
      scope.find_in_batches(batch_size: @batch_size) do |batch_records|
        batch_records.each do |record|
          record_key = "#{record.class.name}_#{record.send(record.class.primary_key)}"
          next if @seen_records[record_key]
          @seen_records[record_key] = true

          collector = StreamingCollector.new(
            @base_class,
            record.send(@base_class.primary_key),
            @dependencies,
            @seen_records
          )

          collector.collect_stream(batch_size: @batch_size) do |records|
            statements = StreamingTranslator.new(records, @configuration).translate
            yield statements unless statements.empty?
          end
        end
      end
    end

    private

    def scope
      @base_class.where(@base_class.primary_key => @id)
    end
  end
end
