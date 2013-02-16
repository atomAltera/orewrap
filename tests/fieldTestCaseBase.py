__author__ = 'Nuclight.atomAltera'

import unittest
from random import Random

from redis import Redis

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

	@classmethod
	def tearDownClass(cls):
		cls.redis.flushdb()

	def setUp(self):
		self.redis.flushdb()

	def r(self, num=1):
		values = set()

		while len(values) < num:
			values.add(self._random.randint(0x100000, 0xFFFFFF))

		result = tuple('{0:X}'.format(value) for value in values)
		return result if num > 1 else result[0]
