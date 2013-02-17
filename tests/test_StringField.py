__author__ = 'Nuclight.atomAltera'

from tests.fieldTestCaseBase import FieldTestCaseBase

from orewrap.fields import StringField



class StringFieldTestCase(FieldTestCaseBase):
	def setUp(self):
		super(StringFieldTestCase, self).setUp()
		self.field = StringField(self.key, redis=self.redis, value_encoder=self.v_con)

	def test_set(self):
		self.redis.set(self.key, self.v_con.encode(self.values[1]))

		result = self.field.set(self.values[2], False)

		self.assertFalse(result)
		self.assertEqual(self.v_con.decode(self.redis.get(self.key)), self.values[1])


		result = self.field.set(self.values[3], True)

		self.assertTrue(result)
		self.assertEqual(self.v_con.decode(self.redis.get(self.key)), self.values[3])


	def test_get(self):
		result = self.field.get(self.values[1])
		self.assertEqual(result, self.values[1])


		self.redis.set(self.key, self.values_c[2])

		result = self.field.get()
		self.assertEqual(result, self.values[2])