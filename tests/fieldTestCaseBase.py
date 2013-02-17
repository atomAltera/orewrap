__author__ = 'Nuclight.atomAltera'

import unittest
from random import Random

from redis import Redis

from orewrap.conversters import ConvertQueue, stringConverter, base64Converter, base32Converter, base16Converter

HOST = 'localhost'
PORT = 6379
DB = 0

KEY = 'test'

class FieldTestCaseBase(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		cls.redis = Redis(host=HOST, port=PORT, db=DB, decode_responses=False)
		cls.key = KEY

		cls._random = Random()
		cls.v_con = ConvertQueue(stringConverter, base64Converter)
		cls.n_con = ConvertQueue(stringConverter, base32Converter)
		cls.s_con = ConvertQueue(stringConverter, base16Converter)

		cls.values = cls.r(30)
		cls.names = cls.r(30)
		cls.scores = cls.r(30)

		cls.value_codes = tuple(map(cls.v_con.encode, cls.values))
		cls.name_codes = tuple(map(cls.n_con.encode, cls.names))
		cls.score_codes = tuple(map(cls.s_con.encode, cls.scores))

	@classmethod
	def tearDownClass(cls):
		cls.redis.flushdb()

	def setUp(self):
		self.redis.flushdb()


	@classmethod
	def r(cls, num=1):
		values = set()

		while len(values) < num:
			values.add(cls._random.randint(0x100000, 0xFFFFFF))

		result = tuple('{0:X}'.format(value) for value in values)
		return result if num > 1 else result[0]
