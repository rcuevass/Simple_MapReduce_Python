import MapReduce
import sys

"""
Social Network dataset with friend-counting using Simple Python MapReduce Framework
Test Input: friends.json
Test Output: friend_count.json

IMPORTANT: This is quite similar, if not identical, to word count

"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: person 
    # value: just a single count indicating record[1] is a friend with 'key'
    key = record[0]
    mr.emit_intermediate(key, 1)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = 0
    for v in list_of_values:
      total += v
    mr.emit((key, total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
