__author__ = 'Nuclight.atomAltera'

from tests.fieldTestCaseBase import FieldTestCaseBase

from orewrap.fields import StringField

class StringFieldTestCase(FieldTestCaseBase):
	def setUp(self):
		super(StringFieldTestCase, self).setUp()

		self.field = StringField(self.key, redis=self.redis)

	def test_set(self):
		value_1, value_2, value_3 = self.r(3)


		self.redis.set(self.key, value_1)

		result = self.field.set(value_2, False)

		self.assertFalse(result)
		self.assertEqual(self.redis.get(self.key), value_1)


		result = self.field.set(value_3, True)

		self.assertTrue(result)
		self.assertEqual(self.redis.get(self.key), value_3)


	def test_get(self):
		value, default = self.r(2)

		result = self.field.get(default)
		self.assertEqual(result, default)


		self.redis.set(self.key, value)

		result = self.field.get()
		self.assertEqual(result, value)