__author__ = 'Nuclight.atomAltera'

from .fields import Field, StringField, ScoreStringField, RefStringField, HashField, SetField, SortedSetField
from .encoders import Encoder, datetime_encoder

from datetime import datetime

def get_object_encoder(object_class):
	def encode(object):
		assert isinstance(object, object_class)
		return str(object)

	def decode(code):
		if code is None: return None
		return object_class.Get(code)

	return Encoder(encode, decode)


class ObjectType(type):
	def __init__(cls, name, bases, fields):
		super(ObjectType, cls).__init__(name, bases, fields)
		cls._Init_Fields()


class Object(metaclass=ObjectType):
	_Name = 'obj'

	@classmethod
	def _Init_Fields(cls):
		cls._Encoder = get_object_encoder(cls)

		cls._Last_Id = StringField(cls._K('last_id'))

		cls._Register = SortedSetField(cls._K('reg'))
		cls.Register = SortedSetField(cls._K('reg'), value_encoder=cls._Encoder, score_encoder=datetime_encoder)

	@classmethod
	def _K(cls, name):
		return "%s:%s" % (cls._Name, name)

	def _k(self, name):
		return "%s:%s:%s" % (self._Name, self.object_id, name)


	@classmethod
	def Create(cls):
		object_id = cls._Last_Id.increment()

		new_object = cls(object_id)
		new_object.create_date.set(datetime.now())

	@classmethod
	def Get(cls, object_id):
		if not cls.Exists(object_id): return None

		return cls(object_id)

	@classmethod
	def Exists(cls, object_id):
		return cls._Register.contains(object_id)

	def __init__(self, object_id):
		assert object_id is not None
		assert str(object_id)

		self.object_id = object_id

		self.create_date = ScoreStringField(self._k('create_date'), self.Register, self, score_encoder=datetime_encoder)
		self.tag = StringField(self._k('tag'))


	def delete(self):
		for value in self.__dict__.values():
			if isinstance(value, Field):
				value.destroy()