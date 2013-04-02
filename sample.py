#!/usr/bin/env python2.7

import config
import pycassa
import itertools
import random

from pprint import pprint
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

############
## Helper functions
############

# helper for random data generation
def elements_from_list(list):
    first = random.randint(1,len(list)/2)
    last = random.randint(first+2,len(list))
    if first == last:
        last += 2
    return list[first:last]

# helper funtion for printing results in a table
def print_result(results):
    l = [x for x in results]
    vals = [x for x in set(list(itertools.chain(*[x[1].keys() for x in l])))]
    string = "%7.7s ||" + " %10.10s |"*len(vals)
    print string % tuple(list(itertools.chain(["Keys"], vals)))
    print "=" * 80
    for x in l:
        v = []
        for y in vals:
            v.append(x[1].get(y,""))
        print string % tuple(list(itertools.chain([x[0]], v)))

#############
## Program Start
#############

read_consistency = pycassa.cassandra.ttypes.ConsistencyLevel.ONE
write_consistency = pycassa.cassandra.ttypes.ConsistencyLevel.ONE
topology = pycassa.system_manager.SIMPLE_STRATEGY


# set up the cassandra object
cass = pycassa.system_manager.SystemManager('localhost')

# Normally you wouldn't drop the keyspace first
# I only do it here to make everything clean
print "Dropping keyspace"
if 'jedberg_test' in cass.list_keyspaces():
    cass.drop_keyspace('jedberg_test')

# create the keyspace
print "Creating keyspace"
cass.create_keyspace('jedberg_test', topology, {'replication_factor': '1'})
cass.ks = 'jedberg_test'
pool = ConnectionPool('jedberg_test')
conn = pool.get()

# create the column families
families = ['collected_properties',
            'collection_cache_by_times',
            'collections_by_cache']

print "Creating column families"
for fam in families:
    cass.create_column_family(cass.ks, fam)
    pass

# Let's see if those keyspaces are set up correctly
print "Keyspaces: "
print cass.list_keyspaces()
print
print "Column families:"
print conn.get_keyspace_description().keys()
print

# set up CF handlers
print "Setting up handlers"
print
cf_handler = {}
for fam in families:
    cf_handler[fam] = pycassa.ColumnFamily(pool, fam, \
            write_consistency_level = write_consistency, \
            read_consistency_level = read_consistency)


# insert some data
print "Inserting data"
print
# first collection run
cf_handler['collection_cache_by_times'].insert('1', {'cache1': 'LAX1', 'cache2': 'LAX2'})
cf_handler['collections_by_cache'].insert('LAX1',{'1': 'LAX1_1'})
cf_handler['collections_by_cache'].insert('LAX2',{'1': 'LAX2_1'})
cf_handler['collected_properties'].insert('LAX1_1', {'up': 'True', 'healthy': 'True', 'other_thing': '6', 'load_avg': '.68'})
cf_handler['collected_properties'].insert('LAX2_1', {'up': 'True', 'healthy': 'False', 'other_thing': '2', 'load_avg': '.18'})

# second run
cf_handler['collection_cache_by_times'].insert('2', {'cache1': 'LAX1', 'cache2': 'LAX2'})
cf_handler['collections_by_cache'].insert('LAX1',{'2': 'LAX1_2'})
cf_handler['collections_by_cache'].insert('LAX2',{'2': 'LAX2_2'})
cf_handler['collected_properties'].insert('LAX1_1', {'up': 'False', 'healthy': 'True', 'other_thing': '3', 'load_avg': '1.68'})
cf_handler['collected_properties'].insert('LAX2_1', {'up': 'True', 'healthy': 'False', 'other_thing': '5', 'load_avg': '.49'})

# third run
cf_handler['collection_cache_by_times'].insert('3', {'cache1': 'SJC1', 'cache2': 'LAX2', 'cache3': 'LAX1'})
cf_handler['collections_by_cache'].insert('SJC1',{'3': 'SJC1_3'})
cf_handler['collections_by_cache'].insert('LAX2',{'3': 'LAX2_3'})
cf_handler['collections_by_cache'].insert('LAX1',{'3': 'LAX1_3'})
cf_handler['collected_properties'].insert('SJC1_3', {'up': 'True', 'healthy': 'False', 'other_thing': '3', 'load_avg': '.01'})
cf_handler['collected_properties'].insert('LAX2_3', {'up': 'False', 'healthy': 'True', 'other_thing': '9', 'load_avg': '.29'})
cf_handler['collected_properties'].insert('LAX1_3', {'up': 'True', 'healthy': 'False', 'other_thing': '2', 'load_avg': '1.13'})

# The above should give you a good idea of what you need to insert
# on each run.  Now we'll do a bunch of random data
print "Generating random data"
print

caches = ['ORD1','ORD2','LAX1','LAX2','LAX3','SJC1','SJC2','JFK1','LAS1']
for c in xrange(4,21):
    these_caches = elements_from_list(caches)
    i = 1
    data = {}
    for cache in these_caches:
        data['%s%s' % ("cache",str(i))] = cache
        i += 1
    cf_handler['collection_cache_by_times'].insert(str(c), data)
    for cache in these_caches:
        cf_handler['collections_by_cache'].insert(cache,{'%s' % c: '%s_%s' % (cache, str(c))})
        cf_handler['collected_properties'].insert('%s_%s' % (cache, str(c)),
                {'up': random.choice(['True','False']),
                'healthy': random.choice(['True','False']),
                'other_thing': str(random.randint(2,9)),
                'load_avg': str("%2.2f" % (random.random()*10))})
        pass




# Let's see what it all looks like
# ps. Using a bare get_range() is genearlly a bad idea
# because it does a full table scan
print "Here's the data"
print
for fam in families:
    print fam
    print_result(cf_handler[fam].get_range())
    print

##################
## Sample queries
##################

## get last 3 runs for SJC2 and show the collected data
# first we get all the timestamps for the cache we are interested in
row = cf_handler['collections_by_cache'].get('SJC2')
# then we sort it
key_list = row.keys()
key_list.sort(lambda a,b: cmp(int(a), int(b)))
# then we return the data for the last three
print "Sample query 1"
for k in key_list[-3:]:
    print "%s: %r" % (row.get(k).split('_')[0], cf_handler['collected_properties'].get(row.get(k)))
print

## what caches did we see on the last run and what are their details?
# presumably we know when the last run is (here it is 20)
print "Sample query 2"
row = cf_handler['collection_cache_by_times'].get('20')
for k in row.keys():
    print "%s: %r" % (row.get(k), cf_handler['collected_properties'].get("%s_%s" % (row.get(k), "20")))
print



