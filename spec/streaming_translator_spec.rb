require 'spec_helper'

describe Polo::StreamingTranslator do

  before(:all) do
    TestData.create_netto
  end

  let(:netto) { AR::Chef.where(name: 'Netto').first }

  it 'translates records to inserts' do
    insert_netto = [%q{INSERT INTO "chefs" ("id", "name", "email") VALUES (1, 'Netto', 'nettofarah@gmail.com');}]
    netto_to_sql = Polo::StreamingTranslator.new([netto]).translate
    expect(netto_to_sql).to eq(insert_netto)
  end

  it 'handles hash records for join tables' do
    hash_record = { table_name: 'recipes_tags', values: { 'recipe_id' => 1, 'tag_id' => 2 } }
    inserts = Polo::StreamingTranslator.new([hash_record]).translate

    expect(inserts.first).to include('recipes_tags')
  end

  describe "options" do
    describe "obfuscate: [fields]" do
      let(:email) { 'nettofarah@gmail.com' }
      let(:translator) { Polo::StreamingTranslator.new([netto], Polo::Configuration.new(obfuscate: obfuscated_fields)) }

      context "obfuscated field with no specified strategy" do
        let(:obfuscated_fields) { { email: nil } }

        it "shuffles characters in field" do
          translator.translate
          expect(netto.email).to_not eq(email)
          expect(netto.email.length).to eq(email.length)
          expect(sorted_characters(netto.email)).to eq(sorted_characters(email))
        end
      end

      context "obfuscated field with custom strategy" do
        let(:obfuscated_fields) { { email: lambda { |_| 'hidden' } } }

        it "replaces contents of field according to the supplied lambda" do
          translator.translate
          expect(netto.email).to eq('hidden')
        end
      end

      def sorted_characters(str)
        str.split("").sort.join
      end
    end
  end
end
