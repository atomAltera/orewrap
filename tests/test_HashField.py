__author__ = 'Nuclight.atomAltera'

from tests.fieldTestCaseBase import FieldTestCaseBase

from orewrap.fields import HashField

class HashFieldTestCase(FieldTestCaseBase):
	def setUp(self):
		super(HashFieldTestCase, self).setUp()

		self.field = HashField(self.keys[0], redis=self.redis, value_encoder=self.v_con, name_encoder=self.n_con)

	def test_set(self):
		self.redis.hset(self.keys[0], self.names_c[1], self.values_c[1])

		result = self.field.set(self.names[1], self.values[2], False)

		self.assertFalse(result)
		self.assertEqual(self.redis.hget(self.keys[0], self.names_c[1]), self.values_c[1])

		result = self.field.set(self.names[1], self.values[3], True)

		self.assertTrue(result)
		self.assertEqual(self.redis.hget(self.keys[0], self.names_c[1]), self.values_c[3])

		result = self.field.set(self.names[2], self.values[1])

		self.assertTrue(result)
		self.assertEqual(self.redis.hget(self.keys[0], self.names_c[2]), self.values_c[1])


	def test_get(self):
		self.redis.hset(self.keys[0], self.names_c[1], self.values_c[1])

		result = self.field.get(self.names[1])

		self.assertEqual(result, self.values[1])

		result = self.field.get(self.names[2], self.values[2])

		self.assertEqual(result, self.values[2])


	def test_set_multi(self):
		result = self.field.set_multi(self.d(self.names[:5], self.values[:5]))

		self.assertTrue(result)
		self.assertSequenceEqual(self.redis.hmget(self.keys[0], self.names_c[:5]), self.values_c[:5])


	def test_get_multi(self):
		self.redis.hmset(self.keys[0], self.d(self.names_c[:5], self.values_c[:5]))

		result = self.field.get_multi(self.names[:5])
		self.assertSequenceEqual(result, self.values[:5])

		result = self.field.get_multi(self.names[:6], self.values[-1])
		self.assertSequenceEqual(result, self.values[:5] + (self.values[-1], ))


	def test_names(self):
		self.redis.hmset(self.keys[0], self.d(self.names_c, self.values_c))

		result = self.field.names()
		self.assertSequenceEqual(sorted(result), sorted(self.names))

	def test_values(self):
		self.redis.hmset(self.keys[0], self.d(self.names_c, self.values_c))

		result = self.field.values()
		self.assertSequenceEqual(sorted(result), sorted(self.values))

	def test_members(self):
		self.redis.hmset(self.keys[0], self.d(self.names_c, self.values_c))

		result = self.field.members()
		self.assertDictEqual(result, self.d(self.names, self.values))

	def test_items(self):
		self.redis.hmset(self.keys[0], self.d(self.names_c, self.values_c))

		result = self.field.items()
		self.assertSequenceEqual(result, self.d(self.names, self.values).items())


	def test_count(self):
		self.redis.hmset(self.keys[0], self.d(self.names_c[:4], self.values_c[:4]))

		result = self.field.count()
		self.assertEqual(result, 4)

	def test_contains(self):
		self.redis.hmset(self.keys[0], self.d(self.names_c[:4], self.values_c[:4]))

		result = self.field.contains(self.names[2])
		self.assertTrue(result)

		result = self.field.contains(self.names[7])
		self.assertFalse(result)

	def test_delete(self):
		result = self.field.delete(self.names[2])
		self.assertEqual(result, 0)

		temp_dict = self.d(self.names_c, self.values_c)
		self.redis.hmset(self.keys[0], temp_dict)

		result = self.field.delete(self.names[5])
		self.assertEqual(result, 1)

		del temp_dict[self.names_c[5]]
		self.assertDictEqual(self.redis.hgetall(self.keys[0]), temp_dict)
