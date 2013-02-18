__author__ = 'Nuclight.atomAltera'

from functools import reduce

class Encoder():
	def __init__(self, encoder=None, decoder=None):
		if encoder is not None: self.encode = encoder
		if decoder is not None: self.decode = decoder

	def encode(self, value):
		return value

	def decode(self, code):
		return code


class EncodeQueue():
	def __init__(self, *encoders):
		filterFunc = lambda func: func is not None

		self._encoders = list(filter(filterFunc, (encoder.encode for encoder in encoders)))
		self._decoders = list(filter(filterFunc, (encoder.decode for encoder in reversed(encoders))))

	def encode(self, value):
		return reduce(lambda value, encoder: encoder(value), self._encoders, value)

	def decode(self, code):
		return reduce(lambda code, decoder: decoder(code), self._decoders, code)


string_encoder = Encoder(
	lambda value: str(value).encode(encoding='utf-8'),
	lambda code: code.decode(encoding='utf-8')
)


lowercase_encoder = Encoder(
	lambda value: str(value).lower(),
	None
)

from datetime import datetime

datetime_encoder = Encoder(
	lambda value: value.timestamp(),
	lambda code: datetime.fromtimestamp(float(code))
)

import base64

base64_encoder = Encoder(base64.b64encode, base64.b64decode)
base32_encoder = Encoder(base64.b32encode, base64.b32decode)
base16_encoder = Encoder(base64.b16encode, base64.b16decode)
