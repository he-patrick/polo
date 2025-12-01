require 'spec_helper'

describe 'Polo.explore_stream' do
  let(:chef_ids) do
    (1..5).map do |i|
      chef = AR::Chef.create(name: "StreamChef #{i}", email: "streamchef#{i}@example.com")
      AR::Recipe.create(title: "StreamRecipe #{i}A", chef: chef)
      AR::Recipe.create(title: "StreamRecipe #{i}B", chef: chef)
      chef.id
    end
  end

  it 'yields INSERT statements in batches' do
    batches = []
    
    Polo.explore_stream(AR::Chef, chef_ids, :recipes, batch_size: 2) do |batch|
      batches << batch
    end
    
    expect(batches.length).to eq(3)
    
    batches.each do |batch|
      expect(batch).to be_an(Array)
      expect(batch.first).to match(/^INSERT INTO/)
    end
  end

  it 'generates correct INSERT statements for chefs and recipes' do
    chef1 = AR::Chef.create(name: 'TestChef 1', email: 'test1@example.com')
    AR::Recipe.create(title: 'TestRecipe 1A', chef: chef1)
    AR::Recipe.create(title: 'TestRecipe 1B', chef: chef1)
    
    chef2 = AR::Chef.create(name: 'TestChef 2', email: 'test2@example.com')
    AR::Recipe.create(title: 'TestRecipe 2A', chef: chef2)
    AR::Recipe.create(title: 'TestRecipe 2B', chef: chef2)
    
    all_inserts = []
    
    Polo.explore_stream(AR::Chef, [chef1.id, chef2.id], :recipes, batch_size: 2) do |batch|
      all_inserts.concat(batch)
    end
    
    chef_inserts = all_inserts.select { |sql| sql.include?('INSERT INTO "chefs"') }
    expect(chef_inserts.length).to eq(2)
    
    recipe_inserts = all_inserts.select { |sql| sql.include?('INSERT INTO "recipes"') }
    expect(recipe_inserts.length).to eq(4)
    
    expect(all_inserts.join).to include("TestChef 1")
    expect(all_inserts.join).to include("TestChef 2")
    expect(all_inserts.join).to include("TestRecipe 1A")
    expect(all_inserts.join).to include("TestRecipe 2B")
  end

  it 'handles single ID as input' do
    chef = AR::Chef.create(name: 'SingleChef', email: 'single@example.com')
    AR::Recipe.create(title: 'Single Recipe', chef: chef)
    
    batches = []
    
    Polo.explore_stream(AR::Chef, chef.id, :recipes, batch_size: 10) do |batch|
      batches << batch
    end
    
    expect(batches.length).to eq(1)
    expect(batches.first).to be_an(Array)
    expect(batches.first.join).to include('SingleChef')
  end

  it 'respects batch_size parameter' do
    batch_sizes = []
    
    Polo.explore_stream(AR::Chef, chef_ids, :recipes, batch_size: 1) do |batch|
      batch_sizes << batch.length
    end
    
    expect(batch_sizes.length).to eq(5)
  end

  it 'works with nested associations' do
    chef = AR::Chef.create(name: 'Nested Chef', email: 'nested@example.com')
    recipe = AR::Recipe.create(title: 'Complex Recipe', chef: chef)
    recipe.ingredients.create(name: 'Ingredient 1', quantity: '1 cup')
    recipe.ingredients.create(name: 'Ingredient 2', quantity: '2 tbsp')
    
    all_inserts = []
    Polo.explore_stream(AR::Chef, chef.id, {:recipes => :ingredients}, batch_size: 10) do |batch|
      all_inserts.concat(batch)
    end
    
    expect(all_inserts.join).to include('Nested Chef')
    expect(all_inserts.join).to include('Complex Recipe')
    expect(all_inserts.join).to include('Ingredient 1')
    expect(all_inserts.join).to include('Ingredient 2')
  end

  it 'handles empty results gracefully' do
    batches = []
    
    Polo.explore_stream(AR::Chef, [], :recipes, batch_size: 10) do |batch|
      batches << batch
    end
    
    expect(batches).to be_empty
  end

  it 'works with obfuscation settings' do
    chef = AR::Chef.create(name: 'ObfuscateChef', email: 'obfuscate@example.com')
    AR::Recipe.create(title: 'Obfuscate Recipe', chef: chef)
    
    Polo.configure do
      obfuscate :email
    end
    
    all_inserts = []
    Polo.explore_stream(AR::Chef, [chef.id], :recipes, batch_size: 10) do |batch|
      all_inserts.concat(batch)
    end
    
    joined = all_inserts.join
    expect(joined).to_not include('obfuscate@example.com')
    expect(joined).to include('ObfuscateChef')
  end

  it 'yields batches separately without accumulating all in memory' do
    yielded_at = []
    
    Polo.explore_stream(AR::Chef, chef_ids, :recipes, batch_size: 2) do |batch|
      yielded_at << Time.now
      sleep(0.01)
    end
    
    expect(yielded_at.length).to be > 1
    
    time_gaps = yielded_at.each_cons(2).map { |t1, t2| t2 - t1 }
    expect(time_gaps.any? { |gap| gap > 0 }).to be true
  end

  it 'handles duplicates across batches when records share associations' do
    shared_tag = AR::Tag.create(name: 'SharedTag')
    
    chef1 = AR::Chef.create(name: 'Chef With Shared Tag 1', email: 'shared1@example.com')
    recipe1 = AR::Recipe.create(title: 'Recipe 1', chef: chef1)
    recipe1.tags << shared_tag
    
    chef2 = AR::Chef.create(name: 'Chef With Shared Tag 2', email: 'shared2@example.com')
    recipe2 = AR::Recipe.create(title: 'Recipe 2', chef: chef2)
    recipe2.tags << shared_tag
    
    all_inserts = []
    
    Polo.explore_stream(AR::Chef, [chef1.id, chef2.id], { recipes: :tags }, batch_size: 1) do |batch|
      all_inserts.concat(batch)
    end
    
    tag_inserts = all_inserts.select { |sql| sql.include?('INSERT INTO "tags"') }
    expect(tag_inserts.length).to eq(1)
    expect(tag_inserts.first).to include('SharedTag')
    
    expect(all_inserts.join).to include('Chef With Shared Tag 1')
    expect(all_inserts.join).to include('Chef With Shared Tag 2')
  end

  it 'does not yield empty batches when all statements are duplicates' do
    chef = AR::Chef.create(name: 'Unique Chef', email: 'unique@example.com')
    AR::Recipe.create(title: 'Unique Recipe', chef: chef)
    
    batch_count = 0
    
    Polo.explore_stream(AR::Chef, [chef.id, chef.id, chef.id], :recipes, batch_size: 1) do |batch|
      batch_count += 1
      expect(batch).not_to be_empty
    end
    
    expect(batch_count).to eq(1)
  end
end
