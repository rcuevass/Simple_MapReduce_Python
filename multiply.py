import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line
def mapper(record):
    matrix= record[0]
    first = record[1]
    j = record[2]
    value = record[3]
    if matrix == 'a':
        for k in range(5):
            mr.emit_intermediate((first,k), (matrix,j,value))
    elif matrix == 'b':
        for i in range(5):
            mr.emit_intermediate((i,j), (matrix,first,value))
         


def reducer(key, list_of_values):
    
    a ={}; b = {}
    for x,y,z in list_of_values:
        if x == 'a':
            a[y] = z
        else: 
            b[y] = z
    total =0
    for k,v in a.iteritems():
        for j,c in b.iteritems():
            if k==j:
               total +=  v*c
                
    mr.emit((key[0],key[1],total))           


# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
