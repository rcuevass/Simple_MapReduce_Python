import MapReduce
import sys



"""
Inverted Index using Simple Python MapReduce Framework
Test Input: books.json
Test Output: inverted_index.json
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # record is a 2-element list
    # key: a word  
    # value: a document that the word appears in
    key = record[0]
    value = record[1]
    words = value.split()
    for w in words:
      # To we assign to each word the document such a word appeard in
      mr.emit_intermediate(w, key)

def reducer(key, list_of_values):
    # key: a document word
    # value: a list of all the documents that 'key' is in, which
    #        can contain duplicates, so we need to de-duplicate 
    #        from list_of_values
    #
    # We set a set to avoid repetition
    seen = set()
    add = seen.add
    mr.emit((key, [s for s in list_of_values if s not in seen and not add(s)]))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
