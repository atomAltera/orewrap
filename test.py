__author__ = 'Nuclight.atomAltera'

from datetime import datetime

from orewrap.encoders import *

text = 'Hello world'

conv = EncodeQueue(string_encoder, base64_encoder)

c = conv.encode(text)


from redis import Redis

r = Redis()

r.set('a', c)
c = r.get('a')

t = conv.decode(c)

print(c, t, sep='\n')

