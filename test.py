__author__ = 'Nuclight.atomAltera'

from datetime import datetime

from orewrap.encoders import *
from orewrap.fields import *

from redis import Redis

r = Redis()

r.flushdb()

SortedSetField.Init(r)

Field.Init(r)

ref = HashField('ref', name_encoder=Encoder(lambda value: str(value).lower()), overwrite=False)

nick1 = RefStringField('n1', ref, 'first user')
nick2 = RefStringField('n2', ref, 'second user')

nick1.set('atomAltera')
nick2.set('IPD')

print(ref.get('atomAltera'))
print(ref.get('IPD'))



scores = SortedSetField('scores')

rating1 = ScoreStringField('r1', scores, 'first users rating')
rating2 = ScoreStringField('r2', scores, 'second users rating')

rating1.set(42344)
rating2.set(3433)


#rating1.destroy()
#nick2.destroy()

for score in scores:
	print(score)