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

import string
import random
import logging

from google.appengine.api import memcache
from google.appengine.ext import db
from google.appengine.runtime import apiproxy_errors


class MemcachedCount(object):
    DELTA_ZERO = 500000   # Allows negative numbers in unsigned memcache

    def __init__(self, name):
        self.key = 'MemcachedCount' + name

    def get_count(self):
        value = memcache.get(self.key)
        if value is None:
            return 0
        else:
            return string.atoi(value) - MemcachedCount.DELTA_ZERO

    def set_count(self, value):
        memcache.set(self.key, str(MemcachedCount.DELTA_ZERO + value))

    def delete_count(self):
        memcache.delete(self.key)

    count = property(get_count, set_count, delete_count)

    def increment(self, incr=1):
        value = memcache.get(self.key)
        if value is None:
            self.count = incr
        elif incr > 0:
            memcache.incr(self.key, incr)
        elif incr < 0:
            memcache.decr(self.key, -incr)

class Counter(object):
    """A counter using sharded writes to prevent contentions.

    Should be used for counters that handle a lot of concurrent use.
    Follows pattern described in Google I/O talk:
        http://sites.google.com/site/io/building-scalable-web-applications-with-google-app-engine

    Memcache is used for caching counts and if a cached count is available, it is
    the most correct. If there are datastore put issues, we store the un-put values
    into a delayed_incr memcache that will be applied as soon as the next shard put
    is successful. Changes will only be lost if we lose memcache before a successful
    datastore shard put or there's a failure/error in memcache.

    Usage:
        hits = Counter('hits')
        hits.increment()
        my_hits = hits.count
        hits.get_count(nocache=True)  # Forces non-cached count of all shards
        hits.count = 6                # Set the counter to arbitrary value
        hits.increment(incr=-1)       # Decrement
        hits.increment(10)
    """
    NUM_SHARDS = 20

    def __init__(self, name):
        self.name = name
        self.memcached = MemcachedCount('Counter' + name)
        self.delayed_incr = MemcachedCount('DelayedIncr' + name)

    def delete(self):
        q = db.Query(CounterShard).filter('name =', self.name)
        shards = q.fetch(limit=Counter.NUM_SHARDS)
        db.delete(shards)

    def get_count_and_cache(self):
        q = db.Query(CounterShard).filter('name =', self.name)
        shards = q.fetch(limit=Counter.NUM_SHARDS)
        datastore_count = 0
        for shard in shards:
            datastore_count += shard.count
        count = datastore_count + self.delayed_incr.count
        self.memcached.count = count
        return count 

    def get_count(self, nocache=False):
        total = self.memcached.count
        if nocache or total is None:
            return self.get_count_and_cache()
        else:
            return int(total)

    def set_count(self, value):
        cur_value = self.get_count()
        self.memcached.count = value
        delta = value - cur_value
        if delta != 0:
            CounterShard.increment(self, incr=delta)

    count = property(get_count, set_count)

    def increment(self, incr=1, refresh=False):
        CounterShard.increment(self, incr)
        self.memcached.increment(incr)


class CounterShard(db.Model):
    name = db.StringProperty(required=True)
    count = db.IntegerProperty(default=0)

    @classmethod
    def increment(cls, counter, incr=1):
        index = random.randint(1, Counter.NUM_SHARDS)
        counter_name = counter.name
        delayed_incr = counter.delayed_incr.count
        shard_key_name = 'Shard' + counter_name + str(index)
        def get_or_create_shard():
            shard = CounterShard.get_by_key_name(shard_key_name)
            if shard is None:
                shard = CounterShard(key_name=shard_key_name, name=counter_name)
            shard.count += incr + delayed_incr
            key = shard.put()
        try:
            db.run_in_transaction(get_or_create_shard)
        except (db.Error, apiproxy_errors.Error), e:
            counter.delayed_incr.increment(incr)
            logging.error("CounterShard (%s) delayed increment %d: %s", 
                          counter_name, incr, e)
            return False
        if delayed_incr:
            counter.delayed_incr.count = 0
        return True
