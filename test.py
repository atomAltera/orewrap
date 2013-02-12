__author__ = 'Nuclight.atomAltera'

from orewrap.field import Field, StringField, HashField
from redis import Redis

r = Redis('server', decode_responses=True, db=99)

Field.Init(r)


h = HashField('tmp')

h['a'] = 10
h['b'] = 10
h['c'] = 'hello'
h['d'] = 'Maxima'
h['e'] = 234

print(h.members())
print(h.items())
