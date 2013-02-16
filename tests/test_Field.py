__author__ = 'Nuclight.atomAltera'

from tests.fieldTestCaseBase import FieldTestCaseBase

from orewrap.fields import Field

class FieldTestCase(FieldTestCaseBase):
	def setUp(self):
		super(FieldTestCase, self).setUp()

		self.field = Field(self.key, redis=self.redis)

	def test_exists(self):
		self.assertFalse(self.field.exists())

		self.redis.set(self.key, 'foobar')

		self.assertTrue(self.field.exists())

	def test_destroy(self):
		self.redis.set(self.key, 'foobar')
		self.field.destroy()

		self.assertFalse(self.redis.exists(self.key))

	def test_equals(self):
		self.assertTrue(self.field == Field(self.key, redis=self.redis))
		self.assertFalse(self.field != Field(self.key, redis=self.redis))
