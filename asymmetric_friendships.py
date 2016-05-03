import MapReduce
import sys

"""
Asymmetric Friendships using simple Python MapReduce framework
Test Input: friends.json
Test Output: asymmetric_friendships.json
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: a sorted tuple of a person and one of his friends
    # value: simply 1. Then we can count in reduce if the total count is 2, which
    #        indicates asymmetric friendship
    person = record[0]
    friend = record[1]
    mr.emit_intermediate(tuple(sorted((person, friend))), 1)

def reducer(key, list_of_values):
    # key: a sorted tuple of a person and one of his friend
    # value: list of occurrence counts
    # output: (person, friend) and (friend, person) for each asymmetric relationships
    total = 0
    for val_ in list_of_values:
        total += val_
    if total == 1:
        mr.emit(key)
        # Reverse sorting...
        mr.emit(key[::-1])


# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
