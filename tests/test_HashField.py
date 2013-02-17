__author__ = 'Nuclight.atomAltera'

from tests.fieldTestCaseBase import FieldTestCaseBase

from orewrap.fields import HashField

class HashFieldTestCase(FieldTestCaseBase):
	def setUp(self):
		super(HashFieldTestCase, self).setUp()

		self.field = HashField(self.key, redis=self.redis, value_encoder=self.v_con, name_encoder=self.n_con)


	def test_set(self):
		self.redis.hset(self.key, self.names_c[1], self.values_c[1])

		result = self.field.set(self.names[1], self.values[2], False)

		self.assertEqual(result, 0)
		self.assertEqual(self.redis.hget(self.key, self.names_c[1]), self.values_c[1])


		result = self.field.set(self.names[1], self.values[3], True)

		self.assertEqual(result, 0)
		self.assertEqual(self.redis.hget(self.key, self.names_c[1]), self.values_c[3])


		result = self.field.set(self.names[2], self.values[1])

		self.assertEqual(result, 1)
		self.assertEqual(self.redis.hget(self.key, self.names_c[2]), self.values_c[1])


	def test_get(self):
		self.redis.hset(self.key, self.names_c[1], self.values_c[1])

		result = self.field.get(self.names[1])
		self.assertEqual(result, self.values[1])

		result = self.field.get(self.names[2], self.values[2])
		self.assertEqual(result, self.values[2])

