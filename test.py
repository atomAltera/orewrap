__author__ = 'Nuclight.atomAltera'

from datetime import datetime

from orewrap.encoders import *
from orewrap.fields import *
from orewrap.object import Object as DbObject

from redis import Redis

r = Redis()
Field.Init(r)
dbObject = DbObject.Create()
