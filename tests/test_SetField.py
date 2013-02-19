__author__ = 'Nuclight.atomAltera'

from tests.fieldTestCaseBase import FieldTestCaseBase

from orewrap.fields import SetField

class SetFieldTestCase(FieldTestCaseBase):
	def setUp(self):
		super(SetFieldTestCase, self).setUp()

		self.field = SetField(self.keys[0], redis=self.redis, value_encoder=self.v_con)

		self.redis.sadd(self.keys[0], *self.values_c[5:])

	def test_add(self):
		result = self.field.add(*self.values[:8])
		self.assertEqual(result, 5)

		temp_set = self.redis.smembers(self.keys[0])
		self.assertSetEqual(temp_set, set(self.values_c[:8]).union(set(self.values_c[5:])))