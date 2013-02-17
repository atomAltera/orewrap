__author__ = 'Nuclight.atomAltera'

from datetime import datetime

from orewrap.conversters import *

text = 'Hello world'

conv = ConvertQueue(stringEncoder, base64Encoder)

c = conv.encode(text)


from redis import Redis

r = Redis()

r.set('a', c)
c = r.get('a')

t = conv.decode(c)

print(c, t, sep='\n')

