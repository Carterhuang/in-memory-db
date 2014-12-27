from imdb import *

db = InMemoryDB()

db.put('a', 10)
db.put('b', 20)
print 'table: ', db.imdb
print 'job queue: ', db.block_stack

db.initiate_block()

db.unset('a')
print 'table: ', db.imdb
print 'job queue: ', [ str(block) for block in db.block_stack ]
