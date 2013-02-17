__author__ = 'Nuclight.atomAltera'

from tests.fieldTestCaseBase import FieldTestCaseBase

from orewrap.fields import HashField

class HashFieldTestCase(FieldTestCaseBase):
	def setUp(self):
		super(HashFieldTestCase, self).setUp()

		self.field = HashField(self.key, redis=self.redis, value_converter=self.v_con, name_converter=self.n_con)


	def test_set(self):
		name_1, name_2, name_3 = self.names[:3]
		value_1, value_2, value_3 = self.values[:3]
		name_1_c, name_2_c, name_3_c = self.name_codes[:3]
		value_1_c, value_2_c, value_3_c = self.value_codes[:3]


		self.redis.hset(self.key, name_1_c, value_1_c)

		result = self.field.set(name_1, value_2, False)

		self.assertEqual(result, 0)
		self.assertEqual(self.redis.hget(self.key, name_1_c), value_1_c)


		result = self.field.set(name_1, value_3, True)

		self.assertEqual(result, 0)
		self.assertEqual(self.redis.hget(self.key, name_1_c), value_3_c)


		result = self.field.set(name_2, value_1)

		self.assertEqual(result, 1)
		self.assertEqual(self.redis.hget(self.key, name_2_c), value_1_c)


	def test_get(self):
		name_1, name_2 = self.names[:2]
		value_1, value_2 = self.values[:2]
		name_1_c, name_2_c = self.name_codes[:2]
		value_1_c, value_2_c = self.value_codes[:2]


		self.redis.hset(self.key, name_1_c, value_1_c)

		result = self.field.get(name_1)
		self.assertEqual(result, value_1)

		result = self.field.get(name_2, value_2)
		self.assertEqual(result, value_2)

