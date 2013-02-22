__author__ = 'Nuclight.atomAltera'

from .encoders import string_encoder

class Field():
	"""
	Base class for representing key in DB
	"""

	# Global redis client instance
	_Redis = None
	_Value_Encoder = None

	@classmethod
	def Init(cls, redis, value_encoder=None):
		"""
		Initialize global Field settings
		Note that for each Field class different settings can be used!

		redis
			redis client, will be used for all field by default

		value_encoder
			value serializer, will be used for all field by default

		"""
		cls._Redis = redis
		cls._Value_Encoder = value_encoder

	def _keys_from_fields(self, fields):
		return {field._key if isinstance(field, Field) else field for field in fields}

	def __init__(self, key, value_encoder=None, redis=None):
		"""
		Initializing new instance of Field

		key
			Redis DB key name

		value_encoder
			I/O value serializer for current instance

		redis
			Redis client for this instance, if None, global client will be used (Defined in `Field.Init`)
		"""
		key = str(key)

		if not len(key):
			raise Exception('Field key has zero length')

		self._key = key
		self._value_encoder = value_encoder or self._Value_Encoder or string_encoder
		self._redis = redis or self._Redis

		if self._redis is None:
			raise Exception('Redis client not specified')

	def exists(self):
		"""
		Checking field exists in DB

		return value
			True, if field exists, else False
		"""
		return self._redis.exists(self._key)

	def destroy(self):
		"""
		Removing field from DB

		return value
			True, if field existed, else False
		"""
		return bool(self._redis.delete(self._key))

	def __eq__(self, other):
		return (type(self) == type(other)) and (self._key == other._key)

	def __ne__(self, other):
		return not self.__eq__(other)


class StringField(Field):
	"""
	Represents string fields in DB
	http://redis.io/commands#string
	"""

	def __init__(self, key, value_encoder=None, redis=None, overwrite=True):
		"""
		overwrite
			whether overwrite values of existing key by default
		"""
		super(StringField, self).__init__(key, value_encoder=value_encoder, redis=redis)

		self._overwrite = overwrite

	def set(self, value, overwrite=None):
		"""
		Writes value to field.

		overwrite
			whether overwrite value of existing key, if None, uses default value, defined while instantiating

		return value
			True if new value has been set, else False
		"""
		overwrite = self._overwrite if overwrite is None else overwrite
		method = self._redis.set if overwrite else self._redis.setnx

		return bool(method(self._key, self._value_encoder.encode(value)))

	def get(self, default=None):
		"""
		Gets value from field. If field does not exists, 'default' will be returned
		"""
		data = self._redis.get(self._key)

		return self._value_encoder.decode(data) if data is not None else default

	def increment(self, amount=1):
		"""
		Increments numeric value in DB by 'amount'. If field does not exists, "1" will be writen in it and returned.
		If field contains non numeric value, `redis.exceptions.ResponseError` will occur
		"""
		return self._redis.incr(self._key, amount)

	def __call__(self, default=None):
		"""
		Magic way to call `StringField.get` method
		"""
		return self.get(default=default)

	def __bool__(self):
		return self.exists()

	def __hash__(self):
		return hash(self._key)


class HashField(Field):
	"""
	Represents hash fields in DB
	http://redis.io/commands#hash
	"""
	_Name_Encoder = None

	@classmethod
	def Init(cls, redis, value_encoder=None, name_encoder=None):
		"""
		As like `Field.Init`, but also initializes default 'name_encoder' for all instances of HashField
		"""
		super(HashField, cls).Init(redis=redis, value_encoder=value_encoder)

		cls._Name_Encoder = name_encoder


	def __init__(self, key, value_encoder=None, name_encoder=None, redis=None, overwrite=True):
		"""
		name_encoder
			I/O name serializer for current instance

		overwrite
			whether overwrite values of existing name in key by default
		"""
		super(HashField, self).__init__(key, value_encoder=value_encoder, redis=redis)

		self._name_encoder = name_encoder or self._Name_Encoder or string_encoder

		self._overwrite = overwrite

	def set(self, name, value, overwrite=None):
		"""
		Writes single name-value pair to DB hash field.

		overwrite
			whether overwrite value in existing name in key, if None, uses default value, defined while instantiating

		return value
			True if new value has been set, else False
		"""
		overwrite = self._overwrite if overwrite is None else overwrite
		method = self._redis.hset if overwrite else self._redis.hsetnx

		return bool(method(self._key,
			self._name_encoder.encode(name),
			self._value_encoder.encode(value)
		)) or overwrite # because hset returns 0 if name-value pair with same name already exists ( http://redis.io/commands/hset )


	def get(self, name, default=None):
		"""
		Gets value from name-value pair in field by 'name'.
		If field or name does not exists, 'default' will be returned
		"""
		value = self._redis.hget(
			self._key, self._name_encoder.encode(name)
		)

		return self._value_encoder.decode(value) if value is not None else default


	def set_multi(self, dictionary):
		"""
		Writes multiple name-value pairs from 'dictionary' to DB hash field.

		return value
			always True
		"""
		return self._redis.hmset(self._key,
			{self._name_encoder.encode(name): self._value_encoder.encode(value) for name, value in dictionary.items()}
		)

	def get_multi(self, names, default=None):
		"""
		Returns list of values, associated with 'names'

		default
			values for not existed names will be replaces by 'default'

		return value
			list of values, associated with 'names' in the same order as requested in 'names'
		"""
		values = self._redis.hmget(self._key,
			(self._name_encoder.encode(name) for name in names)
		)

		return tuple(self._value_encoder.decode(value) if value is not None else default for value in values)


	def members(self):
		"""
		Returns all name-value pairs, stored in hash field
		"""
		return {
		self._name_encoder.decode(name): self._value_encoder.decode(value)
		for name, value in self._redis.hgetall(self._key).items()
		}

	def contains(self, name):
		"""
		Checks whether hash field contains 'name'
		"""
		return self._redis.hexists(self._key, self._name_encoder.encode(name))

	def names(self):
		"""
		Return set of names in hash field
		"""
		return map(self._name_encoder.decode, self._redis.hkeys(self._key))

	def values(self):
		"""
		Return list of value in hash field
		"""
		return map(self._value_encoder.decode, self._redis.hvals(self._key))

	def delete(self, *names):
		"""
		Deletes name-value pairs form hash field by 'name'

		names
			names of name-value to be deleted

		return value
			Number of deleted members
		"""
		return self._redis.hdel(self._key, *(self._name_encoder.encode(name) for name in names))

	def count(self):
		"""
		Returns count of name-value pairs in hash field
		"""
		return self._redis.hlen(self._key)

	def __len__(self):
		return self.count()

	def __iter__(self):
		return self.keys().__iter__()

	def __contains__(self, name):
		return self.contains(name)

	def __getitem__(self, name):
		return self.get(name)

	def __setitem__(self, name, value):
		return self.set(name, value)

	def __delitem__(self, name):
		return self.delete(name)


	# Aliases and methods for capability with `dict`

	def keys(self):
		return self.names()

	def items(self):
		return self.members().items()


class SetField(Field):
	"""
	Represents set fields in DB
	http://redis.io/commands#set
	"""

	def add(self, *values):
		"""
		Adds 'values' to set field

		return value
			the number of elements that were added to the set, not including all the elements already present into the set.
		"""
		return self._redis.sadd(self._key, *map(self._value_encoder.encode, values))

	def delete(self, *values):
		"""
		Removes 'values' from set field

		return value
			the number of members removed from the set field, not including non existing members.
		"""
		return self._redis.srem(self._key, *map(self._value_encoder.encode, values))

	def members(self):
		"""
		Gets all members from set field
		"""
		result = self._redis.smembers(self._key)
		return {self._value_encoder.decode(value) for value in result}

	def count(self):
		"""
		Gets count of members in set field
		"""
		return self._redis.scard(self._key)

	def contains(self, value):
		"""
		Checks whether set field contains 'value'
		"""
		return self._redis.sismember(self._key, self._value_encoder.encode(value))

	def random(self, number=None, unique=True):
		"""
		Return random value(s) form set field

		number
			how many values must be returned (1 by default). Must be positive!

		unique
			if True, result list will contains unique values and limited by 'number'

		return value
			if number is None or 1, single value will be returned,
			else, set, if 'unique' is True, or collection, if False, of values
		"""
		if number:
			if number < 0:
				raise Exception('Number must be positive')

			if not unique:
				number *= -1
				return map(self._value_encoder.decode, self._redis.srandmember(self._key, number))
			else:
				return {self._value_encoder.decode(value) for value in self._redis.srandmember(self._key, number)}

		else:
			return self._value_encoder.decode(self._redis.srandmember(self._key))

	def pop(self, number=None):
		"""
		Returns 'number' of random values and removes them from set field.
		If number of elements in set field less then 'number', length result set will be limited by set field length

		number
			how many values must be popped (1 by default). Must be positive!

		return value
			if number is None or 1, single value will be returned, else set of values
		"""
		if number:
			if number < 0:
				raise Exception('Number must be positive')

			redis = self._redis.pipeline()

			while number > 0:
				redis.spop(self._key)
				number -= 1

			result = redis.execute()
			return {self._value_encoder.decode(value) for value in filter(lambda value: value is not None, result)}
		else:
			return self._value_encoder.decode(self._redis.spop(self._key))

	def union(self, *setField_collection):
		"""
		Returns union of values of current set field  and 'set_fields' fields and decoded with current value encoder

		set_fields
			collection of `SetField` instances or strings (key names). Note, if items is not instance of `Field`,
			str(item) will be used to get key. If key holding the wrong kind of value, `redis.exceptions.ResponseError`
			exception will be occurred

		return value
			unique values from current set field and set fields if 'set_fields'
		"""
		keys = self._keys_from_fields(setField_collection)

		values = self._redis.sunion(self._key, *keys)
		return {self._value_encoder.decode(value) for value in values}

	def intersection(self, *setField_collection):
		"""
		Returns intersected of values of current set field and 'set_fields' fields and decoded with current value encoder

		set_fields
			collection of `SetField` instances or strings (key names). Note, if items is not instance of `Field`,
			str(item) will be used to get key. If key holding the wrong kind of value, `redis.exceptions.ResponseError`
			exception will be occurred

		return value
			intersection of values from current set field and set fields if 'set_fields'
		"""
		keys = self._keys_from_fields(setField_collection)

		values = self._redis.sinter(self._key, *keys)
		return {self._value_encoder.decode(value) for value in values}

	def __len__(self):
		return self.count()

	def __iter__(self):
		return self.members().__iter__()

	def __contains__(self, value):
		return self.contains(value)


class SortedSetField(Field):
	"""
	Represents set fields in DB
	http://redis.io/commands#sorted_set
	"""

	_Score_Encoder = None

	@classmethod
	def Init(cls, redis, value_encoder=None, score_encoder=None):
		"""
		As like `Field.Init`, but also initializes default 'core_encoder' for all instances of SortedSetField
		"""
		super(SortedSetField, cls).Init(redis=redis, value_encoder=value_encoder)

		cls._Score_Encoder = score_encoder


	def __init__(self, key, value_encoder=None, score_encoder=None, redis=None):
		"""
		score_encoder
			I/O score serializer for current instance
		"""
		super(SortedSetField, self).__init__(key, value_encoder=value_encoder, redis=redis)

		self._score_encoder = score_encoder or self._Score_Encoder or string_encoder


	def add(self, value, score):
		"""
		Adds value-score pair to sorted set field. If 'value' already exists in sorted set field, just score will be updated

		return value
			True, if 'value' was added to sorted set field, else False
		"""
		return bool(self.add_multi({value: score}))

	def add_multi(self, dictionary):
		"""
		Adds value-score pairs to sorted set field. If 'value' already exists in sorted set field, just score will be updated

		values
			mapping values to scores

		return value
			the number of elements added to the sorted set field, not including elements already existing for which the score was updated.
		"""
		args = [None] * len(dictionary) * 2
		args[::2] = map(self._value_encoder.encode, dictionary.keys())
		args[1::2] = map(self._score_encoder.encode, dictionary.values())

		return self._redis.zadd(self._key, *args)

	def delete(self, *values):
		"""
		Deletes elements from sorted set field

		values
			collection of values, to be deleted

		return value
			the number of members removed from the sorted set, not including non existing members.
		"""
		return self._redis.zrem(self._key, *map(self._value_encoder.encode, values))

	def count(self, min_score=None, max_score=None):
		"""
		Returns the number of elements in the sorted set field with a score between 'min_score' and 'max_score'.
		If both min_score and max_score are None, number of all elements will be returned

		min_score
			if None, "-inf" will used by default

		max_score
			if None, "+inf" will used by default
		"""
		if min_score is max_score is None:
			return self._redis.zcard(self._key)

		min_score = '-inf' if min_score is None else self._score_encoder.encode(min_score)
		max_score = '+inf' if max_score is None else self._score_encoder.encode(max_score)

		return self._redis.zcount(self._key, min_score, max_score)

	def range_by_index(self, start_index=None, stop_index=None, desc=False):
		"""
		Returns the specified range of elements in the sorted set field.

		start_index
			start index of range (0 by default)

		stop_index
			end index of range (-1 by default)

		desc
			whether elements must be ordered from highest to lowest score (from the lowest to the highest by default)

		"""
		if start_index is None: start_index = 0
		if stop_index is None: stop_index = -1

		result = self._redis.zrange(self._key, start_index, stop_index, desc)

		return tuple(self._value_encoder.decode(value) for value in result)


	def range_by_score(self, min_score=None, max_score=None):
		"""
		Returns the range of elements in the sorted set field with scores between 'min_score' and 'max_score' (both inclusive).

		min_score
			if None, "-inf" will used by default

		max_score
			if None, "+inf" will used by default

		"""
		min_score = '-inf' if min_score is None else self._score_encoder.encode(min_score)
		max_score = '+inf' if max_score is None else self._score_encoder.encode(max_score)

		result = self._redis.zrangebyscore(self._key, min_score, max_score)

		return tuple(self._value_encoder.decode(value) for value in result)

	def get_by_index(self, index):
		result = self.range_by_index(index, index)
		if not result: return None

		return result[0]

	def index_of(self, value):
		"""
		Returns a 0-based value indicating the rank of 'value' in sorted set field

		return value
			None if sorted set field 'value' is not
		"""
		return self._redis.zrank(self._key, self._value_encoder.encode(value))

	def contains(self, value):
		return self.index_of(value) is not None

	def score_of(self, value):
		"""
		Returns a score for value in the sorted set field
		"""
		return self._redis.zscore(self._key, self._value_encoder.encode(value))

	def delete(self, *values):
		"""
		Removes 'values' from sorted set field

		return value
			the number of members removed from the sorted set field, not including non existing members.
		"""
		return self._redis.zrem(self._key, *map(self._value_encoder.encode, values))

	def delete_range_by_index(self, start_index=None, stop_index=None):
		"""
		Removes all elements in the sorted set stored field at key with rank between 'start_index' and 'stop_index'

		return value
			the number of elements removed.
		"""
		if start_index is None: start_index = 0
		if stop_index is None: stop_index = -1

		return self._redis.zremrangebyrank(self._key, start_index, stop_index)

	def delete_range_by_score(self, min_score=None, max_score=None):
		"""
		Removes all elements in the sorted set stored field at key with a score between 'min_score' and 'max_score' (inclusive).

		return value
			the number of elements removed.
		"""
		min_score = '-inf' if min_score is None else self._score_encoder.encode(min_score)
		max_score = '+inf' if max_score is None else self._score_encoder.encode(max_score)

		return self._redis.zremrangebyscore(self._key, min_score, max_score)

	def __len__(self):
		return self.count()

	def __iter__(self):
		return self.range_by_index().__iter__()

	def __contains__(self, value):
		return self.contains(value)

	def __getitem__(self, value):
		return self.score_of(value)

	def __setitem__(self, value, score):
		return self.add(value, score)

	def __delitem__(self, value):
		return self.delete(value)



class RefStringField(StringField):
	def __init__(self, key, target_hashField, target_hashField_value, name_encoder=None, redis=None):
		super(RefStringField, self).__init__(key, value_encoder=name_encoder, redis=redis, overwrite=True)

		self._target_hashField = target_hashField
		self._target_hashField_value = target_hashField_value

	def set(self, name):
		if not self._target_hashField.set(name, self._target_hashField_value):
			return False

		old_value = self.get()

		super(RefStringField, self).set(name)

		if old_value is not None:
			self._target_hashField.delete(name)

		return True

	def destroy(self):
		name = self.get()

		if name is not None:
			self._target_hashField.delete(name)

		super(RefStringField, self).destroy()




class ScoreStringField(StringField):
	def __init__(self, key, target_sortedSetField, target_sortedSetField_value, score_encoder=None, redis=None):
		super(ScoreStringField, self).__init__(key, value_encoder=score_encoder, redis=redis, overwrite=True)

		self._target_sortedSetField = target_sortedSetField
		self._target_sortedSetField_value = target_sortedSetField_value

	def set(self, score):
		self._target_sortedSetField.add(self._target_sortedSetField_value, score)
		super(ScoreStringField, self).set(score)


	def destroy(self):
		self._target_sortedSetField.delete(self._target_sortedSetField_value)

		super(ScoreStringField, self).destroy()
