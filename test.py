__author__ = 'Nuclight.atomAltera'

from orewrap.field import Field, StringField, HashField
from redis import Redis

Field.Init(Redis(decode_responses=True))


h = HashField('tmp')

h['a'] = 10
h['b'] = 10
h['c'] = 'hello'
h['d'] = 'Maxima'
h['e'] = 234

print(h.members())
print(h.items())
