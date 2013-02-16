__author__ = 'Nuclight.atomAltera'

from datetime import datetime
from base64 import b64encode, b64decode

class Serializer():
	def dump(self, value):
		return value

	def load(self, data):
		return data

class StringSerializer(Serializer):
	def dump(self, value):
		return str(value).encode(encoding='utf-8', errors='strict')

	def load(self, data):
		return data.decode(encoding='utf-8', errors='strict')

class LowerCaseSerializer(Serializer):
	def dump(self, value):
		return super(LowerCaseSerializer, self).dump(str(value).lower())


class DateTimeSerializer(Serializer):
	def dump(self, value):
		assert isinstance(value, datetime)

		return super(DateTimeSerializer, self).dump(
			value.timestamp()
		)

	def load(self, data):
		timestamp = float(
			super(DateTimeSerializer, self).load(data)
		)

		return datetime.fromtimestamp(timestamp)

class Base64Serializer(Serializer):
	def dump(self, value):
		return b64encode(value.encode())

	def load(self, data):
		return b64decode(data).decode()


serializer = Serializer()
stringSerializer = StringSerializer()
lowerCaseSerializer = LowerCaseSerializer()
dateTimeSerializer = DateTimeSerializer()
base64Serializer = Base64Serializer()