__author__ = 'Nuclight.atomAltera'

from datetime import datetime

class Serializer():
	def dump(self, value):
		return value

	def load(self, data):
		return data

class StringSerializer():
	def dump(self, value):
		return str(value).encode(encoding='utf-8', errors='strict')

	def load(self, data):
		return data.decode(encoding='utf-8', errors='strict')

class IgnoreCaseSerializer(Serializer):
	def dump(self, value):
		return super(IgnoreCaseSerializer, self).dump(str(value).lower())


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


serializer = Serializer()
ignoreCaseSerializer = IgnoreCaseSerializer()
dateTimeSerializer = DateTimeSerializer()
