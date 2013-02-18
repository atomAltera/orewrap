__author__ = 'Nuclight.atomAltera'

from tests.fieldTestCaseBase import FieldTestCaseBase

from orewrap.fields import Field

class FieldTestCase(FieldTestCaseBase):
	def setUp(self):
		super(FieldTestCase, self).setUp()

		self.field = Field(self.keys[0], redis=self.redis)

	def test_exists(self):
		result = self.field.exists()
		self.assertFalse(result)

		self.redis.set(self.keys[0], 'foobar')

		result = self.field.exists()
		self.assertTrue(result)

	def test_destroy(self):
		result = self.field.destroy()
		self.assertFalse(result)

		self.redis.set(self.keys[0], 'foobar')

		result = self.field.destroy()
		self.assertTrue(result)

		self.assertFalse(self.redis.exists(self.keys[0]))

	def test_equals(self):
		self.assertTrue(self.field == Field(self.keys[0], redis=self.redis))
		self.assertFalse(self.field != Field(self.keys[0], redis=self.redis))
