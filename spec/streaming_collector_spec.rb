require 'spec_helper'

describe Polo::StreamingCollector do

  before(:all) do
    TestData.create_netto
  end

  let(:netto) { AR::Chef.where(name: 'Netto').first }

  it 'yields the base record' do
    collected = []
    collector = Polo::StreamingCollector.new(AR::Chef, netto.id, {}, {})
    collector.collect_stream { |records| collected.concat(records) }

    expect(collected).to include(netto)
  end

  it 'yields associated records' do
    collected = []
    collector = Polo::StreamingCollector.new(AR::Chef, netto.id, :recipes, {})
    collector.collect_stream { |records| collected.concat(records) }

    expect(collected.any? { |r| r.is_a?(AR::Recipe) }).to be true
  end

  it 'tracks seen records to prevent duplicates' do
    recipe = netto.recipes.first
    seen = { "AR::Recipe_#{recipe.id}" => true }

    collected = []
    collector = Polo::StreamingCollector.new(AR::Chef, netto.id, :recipes, seen)
    collector.collect_stream { |records| collected.concat(records) }

    expect(collected).to_not include(recipe)
  end

  it 'handles nested associations' do
    collected = []
    collector = Polo::StreamingCollector.new(AR::Chef, netto.id, { recipes: :ingredients }, {})
    collector.collect_stream { |records| collected.concat(records) }

    expect(collected.any? { |r| r.is_a?(AR::Ingredient) }).to be true
  end
end

