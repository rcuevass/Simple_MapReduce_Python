import MapReduce
import sys

"""
Relational Join using Simple Python MapReduce Framework
Test Input: records.json
Test Output: join.json
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # record is database records. Specifically for the provided dataset:
    #    * First item(index 0) in each record identifies which table the record originates from
    #       'line_item' indicates that the record is a line item
    #       'order' indicates that the record is an order
    #    * Second element is the order id that we want to join on
    # LineItem records have 17 elements including the identifier string
    # Order records have 10 elements including the identifier string
    #
    # key - the order id 
    # value - the entire record, for joining purpose
    key = record[1]
    mr.emit_intermediate(key, record)

def reducer(key, list_of_values):
    # key: order id
    # value: a list of records with the order id 'key'
    for val_1 in list_of_values:
        for val_2 in list_of_values:
            # We filter duplicate data (motiaved by SQL trick)
            if val_1[0] > val_2[0]: 
                sum_ = val_1 + val_2 
                mr.emit(sum_)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
