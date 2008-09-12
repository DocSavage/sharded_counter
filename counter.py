# Copyright 2008 William T Katz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# THIS LICENSE INFORMATION/ATTRIBUTION must be left in place.

import random
import logging

from google.appengine.api import memcache
from google.appengine.ext import db


class Counter(object):
    """A counter using sharded writes to prevent contentions.

    This is one way of implementing counters based on a Google best practices
    talk:
    http://sites.google.com/site/io/building-scalable-web-applications-with-google-app-engine

    It shards a counter to decrease contentions on writes.  Rather than use a
    Model for the counter configuration as in the Google example, this version
    uses a memcached counter that can be fed with an expandable shard count.

    Memcache is used for caching counts, although you can force non-cached 
    counts.

    Usage:
        hits = Counter('hits')
        hits.increment()
        my_hits = hits.count
        hits.get_count(nocache=True)  # Forces non-cached count.
        hits.decrement()
        hits.delete()                 # Deletes all associated shards.
    """
    MAX_SHARDS = 50

    def __init__(self, name, num_shards=5, cache_time=30):
        self.name = name
        self.num_shards = min(num_shards, Counter.MAX_SHARDS)
        self.cache_time = cache_time

    def delete(self):
        q = db.Query(CounterShard).filter('name =', self.name)
        # Need to use MAX_SHARDS since current number of shards
        # may be smaller than previous value.
        shards = q.fetch(limit=Counter.MAX_SHARDS)
        for shard in shards:
            shard.delete()

    def memcache_key(self):
        return 'Counter' + self.name

    def get_count(self, nocache=False):
        total = memcache.get(self.memcache_key())
        if nocache or total is None:
            total = 0
            q = db.Query(CounterShard).filter('name =', self.name)  
            shards = q.fetch(limit=Counter.MAX_SHARDS)
            for shard in shards:
                total += shard.count
            memcache.add(self.memcache_key(), str(total), 
                         self.cache_time)
            return total
        else:
            return int(total)
    count = property(get_count)

    def increment(self):
        CounterShard.increment(self.name, self.num_shards)
        return memcache.incr(self.memcache_key()) 

    def decrement(self):
        CounterShard.increment(self.name, self.num_shards, 
                               downward=True)
        return memcache.decr(self.memcache_key()) 

class CounterShard(db.Model):
    name = db.StringProperty(required=True)
    count = db.IntegerProperty(default=0)

    @classmethod
    def increment(cls, name, num_shards, downward=False):
        index = random.randint(1, num_shards)
        shard_key_name = 'Shard' + name + str(index)
        def get_or_create_shard():
            shard = CounterShard.get_by_key_name(shard_key_name)
            if shard is None:
                shard = CounterShard(key_name=shard_key_name, 
                                     name=name)
            if downward:
                shard.count -= 1
            else:
                shard.count += 1
            key = shard.put()
        try:
            db.run_in_transaction(get_or_create_shard)
            return True
        except db.TransactionFailedError():
            logging.error("CounterShard (%s, %d) - can't increment", 
                          name, num_shards)
            return False

