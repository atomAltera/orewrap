__author__ = 'Nuclight.atomAltera'

import unittest
from random import Random

from redis import Redis

from orewrap.encoders import EncodeQueue, string_encoder, base64Encoder, base32Encoder, base16Encoder

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
		cls.v_con = EncodeQueue(string_encoder, base64Encoder)
		cls.n_con = EncodeQueue(string_encoder, base32Encoder)
		cls.s_con = EncodeQueue(string_encoder, base16Encoder)

		cls.values = cls.r(10)
		cls.names = cls.r(10)
		cls.scores = cls.r(10, True)

		cls.values_c = tuple(map(cls.v_con.encode, cls.values))
		cls.names_c = tuple(map(cls.n_con.encode, cls.names))
		cls.scores_c = tuple(map(cls.s_con.encode, cls.scores))

	@classmethod
	def tearDownClass(cls):
		cls.redis.flushdb()

	def setUp(self):
		self.redis.flushdb()


	@classmethod
	def r(cls, num=1, integer=False):
		result = set()

		while len(result) < num:
			result.add(cls._random.randint(0x100000, 0xFFFFFF))

		if not integer:
			result = tuple('{0:X}'.format(value) for value in result)

		return result if num > 1 else result[0]
