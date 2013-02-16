__author__ = 'Nuclight.atomAltera'

from tests.fieldTestCaseBase import FieldTestCaseBase

from orewrap.fields import HashField

class HashFieldTestCase(FieldTestCaseBase):
	def setUp(self):
		super(HashFieldTestCase, self).setUp()

		self.field = HashField(self.key, redis=self.redis)


	def test_set(self):
		name_1, name_2 = self.r(2)
		value_1, value_2, value_3 = self.r(3)

		self.redis.hset(self.key, name_1, value_1)


		result = self.field.set(name_1, value_2, False)

		self.assertEqual(result, 0)
		self.assertEqual(self.redis.hget(self.key, name_1), value_1)


		result = self.field.set(name_1, value_3, True)

		self.assertEqual(result, 0)
		self.assertEqual(self.redis.hget(self.key, name_1), value_3)


		result = self.field.set(name_2, value_1)

		self.assertEqual(result, 1)
		self.assertEqual(self.redis.hget(self.key, name_2), value_1)


	def test_get(self):
		name_1, name_2 = self.r(2)
		value_1, value_2 = self.r(2)

		self.redis.hset(self.key, name_1, value_1)

		result = self.field.get(name_1)
		self.assertEqual(result, value_1)

		result = self.field.get(name_2, value_2)
		self.assertEqual(result, value_2)

