import IPython.parallel
import itertools 

# Functions get pickled to be communicated to the engines.
# That's why they have to do their own imports inside the function
def applyStore(key, data):
    """Store data to key in an engine"""
    import storage
    if key in storage.data:
        storage.data[key].append(data)
    else:
        storage.data[key] = []
        storage.data[key].append(data)
    return 0

def applyClean():
    """Clean all storage from an engine"""
    import storage
    for key in storage.data.keys():
        del storage.data[key]
    return 0

def applyCleanKey(akey):
    """Clean a key's storage from an engine"""
    import storage
    if akey in storage.data:
        del storage.data[akey]
    return 0

def applyGetStore(key):
    """Get a key's storage from an engine.
    
    Mostly used for debugging."""
    import storage
    if key in storage.data:
        return storage.data[key]
    else:
        return []


class Engines:
    """A class that distributes and cleans lines of text files from the engines"""
    def __init__(self):
        self.c = IPython.parallel.Client()

    def getStore(self, eid, key):
        """Get the storage in key from engine number eid"""
        r = self.c[eid].apply_async(applyGetStore, key)
        ans = r.get()
        return ans
 
    def cleanKey(self, fname):
        """Cleans all data stored in a key in all the engines."""
        tmp = []
        for e in self.c:
            r = e.apply_async(applyCleanKey, fname)
            tmp.append(r)
        ans = 0
        for r in tmp:
            ans += r.get()
        print 'Cleaned', fname, ans

    def cleanData(self):
        """Cleans all data stored in the engines."""
        execs = []
        for e in self.c:
            r = e.apply_async(applyClean)
            execs.append(r)
        ans = 0
        for r in execs:
            ans += r.get()
        print 'Cleaned', ans
            

    def distributeData(self, aid, afile):
        """Distributes the lines in a file to the different engines."""
        lines = []
        for aline,e in zip(afile.readlines(), itertools.cycle(self.c)):
            r = e.apply_async(applyStore, aid, aline)
            lines.append(r)
        ans = 0
        for r in lines:
            ans += r.get()
        print 'Distributed', ans


def applyMap(key, afunc):
    """Calls mapper in each engine, so that mapping happens in a distributed way"""
    import storage
    ans = []
    for line in storage.data[key]:
        ans.append(afunc(line))
    return ans


class MapReduce:
    """A class for executing map reduce calls."""
    def __init__(self):
        self.e = Engines()
        self.c = IPython.parallel.Client()

    def mapreduce(self, fname, mapper, reducer):
        """Execute a map reduce call.  Takes three parameters

        fname -- the file name to distribute to the engines
        mapper -- the mapper function, executed on the engines
        reduer -- the reducer function, executed in the main program"""
        #Distribute the text file
        self.e.cleanKey(fname)
        self.e.distributeData(fname, open(fname))
        self.fname = fname
        self.mapper = mapper
        self.reducer = reducer
        #Map in a distributed way
        sep = []
        for d in self.c:
            r = d.apply_async(applyMap, self.fname, self.mapper)
            sep.append(r)
        ans = []
        for r in sep:
            ans.extend(r.get())
        ans_dict = {}
        #Turn the lists into a dictionary to be efficient in reducing
        for alist in ans:
            for k,v in alist:
                if k in ans_dict:
                    ans_dict[k].append(v)
                else:
                    ans_dict[k] = []
                    ans_dict[k].append(v)
        #Reduce and return the result
        retval = []
        for k,v in ans_dict.iteritems():
            retval.append( self.reducer(k,v) )
        return retval


if __name__ == '__main__':
    pass
