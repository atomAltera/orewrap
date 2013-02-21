____author__ = 'Nuclight.atomAltera'

from tests.fieldTestCaseBase import FieldTestCaseBase

from orewrap.fields import SortedSetField

T = 10

class SortedSetFieldTestCase(FieldTestCaseBase):
	@classmethod
	def d2l(cls, seq1, seq2):
		d = cls.d(seq1, seq2)
		l = []

		for items in d.items():
			l.append(items[0])
			l.append(items[1])

		return l

	def setUp(self):
		super(SortedSetFieldTestCase, self).setUp()

		self.field = SortedSetField(self.keys[0], redis=self.redis, value_encoder=self.v_con, score_encoder=self.s_con)

		self.redis.zadd(self.keys[0], *self.d2l(self.values_c[T:], self.scores_c[T:]))


	def test_add(self):
		result = self.field.add(self.values[0], self.scores[0])

		self.assertTrue(result)
		self.assertEqual(self.redis.zscore(self.keys[0], self.values_c[0]), self.scores_c[0])

		result = self.field.add(self.values[T + 1], self.scores[T + 1])

		self.assertFalse(result)
		self.assertEqual(self.redis.zscore(self.keys[0], self.values_c[T + 1]), self.scores_c[T + 1])


	def test_add_multi(self):
		result = self.field.add_multi(self.d(self.values[:T + 2], self.scores[:T + 2]))

		self.assertEqual(result, T)
		self.assertSequenceEqual(sorted(self.redis.zrange(self.keys[0], 0, -1)), sorted(self.values_c))


