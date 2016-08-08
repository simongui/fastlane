<img src="fastlane.png"/>
[![Build Status](https://travis-ci.org/simongui/fastlane.svg?branch=master)](https://travis-ci.org/simongui/fastlane)

Fastlane is a caching transport that takes changes from a MySQL database and applies them to an in-memory cache like `memcached` or `redis`. Fastlane is unique because it replicates off of a MySQL database just like a normal MySQL replica does. This allows fastlane to be transactionally aware and sequentially consistent.

# Why fastlane?
Many of todays web services written in node.js, rails or any other language store hot data in in-memory caching services like `memcached` or `redis` for performance to reduce load on databases. Some of the challenges in this caching approach are as follows.

1. Keeping cached values up to date (cache invalidation).
1. Not losing updates between the database (`MySQL`, `MSSQL`, `Oracle`, `Postgres`) and the caching service (`memcached`, `redis`, etc).
1.  Sequential consistency.
1.  Expiring data (TTL's).

Fastlane solves the first 3 problems.

# Cache invalidation and resiliency
One approach to solving `#1` and `#2` above is to put a queue or log of changes in between the database and caching service so that you can replay the log to keep the caching service up to date. If a change fails you just replay from that position in the queue or log and you can retry until it succeeds.

MySQL binlog replication is essentially a replicated queue so fastlane inherits this without adding any additional infrastructure like Kafka because it supports the MySQL replication protocol and can replicate directly from MySQL.

# Sequential consistency
It is far too common to see cached values in `memcached` or `redis` set directly in the web application code. The changes made in MySQL will follow ACID transactional semantics but changes to the cache don't enforce any consistency guarantees. This can cause a user experience where a user made multiple changes but the web application only shows partial changes.

Fastlane solves this problem as well by being a native MySQL replica. Fastlane is aware of MySQL transactions and can group atomic writes if the caching service supports multiple write operations. The MySQL binlog replication protocol is an ordered log and this allows fastlane to always apply changes in a predictable order that matches with the primary source of data in MySQL.
