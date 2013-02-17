__author__ = 'Nuclight.atomAltera'

from datetime import datetime

from orewrap.conversters import *

now = datetime.now()

print(now)

print('-' * 20)

converter = ConvertQueue(dateTimeConverter, stringConverter, base64Converter)

code = converter.encode(now)

print('Code: ', code)
value = converter.decode(code)

print('Value', value, 'Type:', type(value))
