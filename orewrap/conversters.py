__author__ = 'Nuclight.atomAltera'

class Converter():
	def __init__(self, encoder, decoder):
		self._encoder = encoder
		self._decoder = decoder

	def encode(self, value):
		return self._encoder(value)

	def decode(self, code):
		return self._decoder(code)


class ConvertQueue():
	def __init__(self, *converters):
		filterFunc = lambda func: func is not None

		self._encoders = list(filter(filterFunc, (converter.encode for converter in converters)))
		self._decoders = list(filter(filterFunc, (converter.decode for converter in reversed(converters))))

	def encode(self, value):
		code = value

		for encoder in self._encoders:
			code = encoder(code)

		return code

	def decode(self, code):
		value = code

		for decoder in self._decoders:
			value = decoder(value)

		return value


stringConverter = Converter(str.encode, bytes.decode)
lowerCaseConverter = Converter(
	lambda value: str(value).lower(),
	None
)