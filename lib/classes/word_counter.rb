class WordCounter < MapReduce::Mapper
  def self.map(map_data)
    map_data.split.map do |word|
      yield(word.downcase, 1)
    end
  end
end
