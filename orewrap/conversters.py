__author__ = 'Nuclight.atomAltera'

from functools import reduce

class Converter():
	def __init__(self, encoder=None, decoder=None):
		if encoder is not None: self.encode = encoder
		if decoder is not None: self.decode = decoder

	def encode(self, value):
		return value

	def decode(self, code):
		return code


class ConvertQueue():
	def __init__(self, *converters):
		filterFunc = lambda func: func is not None

		self._encoders = list(filter(filterFunc, (converter.encode for converter in converters)))
		self._decoders = list(filter(filterFunc, (converter.decode for converter in reversed(converters))))

	def encode(self, value):
		return reduce(lambda value, encoder: encoder(value), self._encoders, value)

	def decode(self, code):
		return reduce(lambda code, decoder: decoder(code), self._decoders, code)


stringConverter = Converter(
	lambda value: str(value).encode(encoding='utf-8'),
	lambda code: code.decode(encoding='utf-8')
)


lowerCaseConverter = Converter(
	lambda value: str(value).lower(),
	None
)

from datetime import datetime

dateTimeConverter = Converter(
	lambda value: value.timestamp(),
	lambda code: datetime.fromtimestamp(float(code))
)

import base64

base64Converter = Converter(base64.b64encode, base64.b64decode)
base32Converter = Converter(base64.b32encode, base64.b32decode)
base16Converter = Converter(base64.b16encode, base64.b16decode)
