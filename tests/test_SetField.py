__author__ = 'Nuclight.atomAltera'

from tests.fieldTestCaseBase import FieldTestCaseBase

from orewrap.fields import SetField

T = 10

class SetFieldTestCase(FieldTestCaseBase):
	def setUp(self):
		super(SetFieldTestCase, self).setUp()

		self.field = SetField(self.keys[0], redis=self.redis, value_encoder=self.v_con)

		self.redis.sadd(self.keys[0], *self.values_c[T:])

	def test_add(self):
		result = self.field.add(*self.values[:T + 3])
		self.assertEqual(result, T)

		temp_set = self.redis.smembers(self.keys[0])
		self.assertSetEqual(temp_set, set(self.values_c[:T + 3]).union(set(self.values_c[T:])))

	def test_delete(self):
		result = self.field.delete(*self.values[:T + 3])
		self.assertEqual(result, 3)

		temp_set = self.redis.smembers(self.keys[0])
		self.assertSetEqual(temp_set, set(self.values_c[T + 3:]))

	def test_members(self):
		result = self.field.members()

		self.assertSetEqual(result, set(self.values[T:]))

	def test_count(self):
		result = self.field.count()

		self.assertEqual(result, len(set(self.values[T:])))

	def test_contains(self):
		result = self.field.contains(self.values[T - 2])
		self.assertFalse(result)

		result = self.field.contains(self.values[T + 2])
		self.assertTrue(result)

	def test_random(self):
		result = self.field.random()
		self.assertTrue(result in set(self.values[T:]))

		result = self.field.random(4)
		self.assertSetEqual(set(result), set(self.values[T:]).intersection(result))

		result = tuple(self.field.random(30, unique=False))

		self.assertEqual(len(result), 30)
		self.assertSetEqual(set(result), set(self.values[T:]).intersection(result))


	def test_pop(self):
		result = tuple(self.field.pop(3))
		self.assertEqual(len(result), 3)
		self.assertEqual(len(set(result)), 3)

		temp_set = self.redis.smembers(self.keys[0])
		self.assertEqual(len(set(result).intersection(temp_set)), 0)

		self.assertSetEqual(set(self.values_c[T:]).difference(temp_set), {self.v_con.encode(value) for value in result})

		result = self.field.pop()
		self.assertFalse(result in self.redis.smembers(self.keys[0]))

	def test_union(self):
		field1 = SetField(self.keys[1], redis=self.redis)

		self.redis.sadd(self.keys[1], *self.values_c[:2])
		self.redis.sadd(self.keys[2], *self.values_c[2:T + 3])

		result = self.field.union(field1, self.keys[2])

		self.assertSetEqual(result,
			set(self.values[T:]).union(set(self.values[:2]), set(self.values[2:T + 3]))
		)

	def test_intersection(self):
		field1 = SetField(self.keys[1], redis=self.redis)

		self.redis.sadd(self.keys[1], *self.values_c[:T + 3])
		self.redis.sadd(self.keys[2], *self.values_c[2:T + 1])

		result = self.field.intersection(field1, self.keys[2])

		self.assertSetEqual(result,
			set(self.values[T:]).intersection(set(self.values[:T + 3]), set(self.values[2:T + 1]))
		)