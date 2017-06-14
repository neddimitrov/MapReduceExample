import mapred


#All these get executed on the engines, so they have to do their imports inside the function call
def wmapper(line):
    """Mapper for the word example"""
    import re
    WORD_RE = re.compile(r"[\w']+")
    ans = []
    for word in WORD_RE.findall(line):
        ans.append((word, 1))
    return ans
  
def wreducer(key, entries):
    """Reducer for the word example"""
    return key, sum(entries)

def mmapper(line):
    """Mapper for the movies.txt file"""
    pairs = line[1:-2].split(',')
    pairs = [x.split(':') for x in pairs]
    pairs = [(x[0].strip(), x[1].strip()) for x in pairs]
    adict = dict(pairs)
    ans = [(adict['name'], adict['id'])]
    return ans

def mreducer(key, entries):
    """Reducer for the movies.txt file"""
    return key, entries[0]

def rmapper(line):
    """Mapper for the ratings.txt file"""
    pairs = line[1:-2].split(',')
    pairs = [x.split(':') for x in pairs]
    pairs = [(x[0].strip(), x[1].strip()) for x in pairs]
    adict = dict(pairs)
    ans = [(adict['movie_id'], float(adict['rating']))]
    return ans

def rreducer(key, entries):
    """Reducer for the ratings.txt file"""
    return key, sum(entries)/float(len(entries))


def jmapper(line):
    """Mapper for the newfile.txt file.  Basically using map reduce to do the join."""
    import ast
    pair = ast.literal_eval(line)
    if type(pair[1]) == str:
        return [(pair[1], pair[0])]
    else:
        return [pair]

def jreducer(key, entries):
    """Reducer for the newfile.txt file.  Basically using map reduce to do the join."""
    if type(entries[0]) == str:
        return entries[0], entries[1]
    else:
        return entries[1], entries[0]

if __name__ == '__main__':
    mr = mapred.MapReduce()
    example1 = mr.mapreduce('if-kipling.txt', wmapper, wreducer)
    print 'Words - Example 1'
    print '----------------'
    print example1
    print 
    print
    movies = mr.mapreduce('movies.txt', mmapper, mreducer)
    print 'Movies'
    print '------'
    print movies
    print 
    print
    ratings = mr.mapreduce('ratings.txt', rmapper, rreducer)
    print 'Ratings'
    print '-------'
    print ratings
    print
    print
    # Diogo said "No Outside Joins are allowed."
    # I think this an efficient solution and a map reduce
    # program that solves the problem, so I'm keeping it here 
    # for the purposes of conversation.  I'll also solve the 
    # problem a different way with map and reduce and creating 
    # a file for another map and reduce
    print 'Movie Name, Rating - Example 2'
    print '------------------------------'
    ratings = dict(ratings)
    for k,v in dict(movies).iteritems():
        print '%s, %f'%(k,ratings[v])
    print
    print
    ######
    movies = mr.mapreduce('movies.txt', mmapper, mreducer)
    print 'Movies (2.0)'
    print '------------'
    print movies
    print 
    print
    ratings = mr.mapreduce('ratings.txt', rmapper, rreducer)
    print 'Ratings (2.0)'
    print '-------------'
    print ratings
    print
    print
    # Write out the previous two map reduce steps.
    # The lines are in arbitrary order
    ofile = open('newfile.txt','w')
    for m in movies:
        ofile.write(str(m)+'\n')
    for r in ratings:
        ofile.write(str(r)+'\n')
    ofile.close()
    # Now use a map reduce step to do the "join."
    named_ratings = mr.mapreduce('newfile.txt', jmapper, jreducer)
    print 'MR Joined Ratings (2.0)'
    print '-----------------------'
    print named_ratings
    print
    print

