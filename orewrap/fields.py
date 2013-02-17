__author__ = 'Nuclight.atomAltera'

from .conversters import ConvertQueue, stringConverter

class Field():
	"""
	Base class for representing key in DB
	"""

	# Global redis client instance
	_Redis = None
	_Value_Converter = None

	@classmethod
	def Init(cls, redis, value_converter = None):
		"""
		Initialize global Field settings
		Note that for each Field class different settings can be used!

		redis
			redis client, will be used for all field by default

		value_converter
			value serializer, will be used for all field by default

		"""
		cls._Redis = redis
		cls._Value_Converter = value_converter

	def __init__(self, key, value_converter=None, redis=None):
		"""
		Initializing new instance of Field

		key
			Redis DB key name

		value_converter
			I/O 'value' serializer for current instance

		redis
			Redis client for this instance, if None, global client will be used (Defined in `Field.Init`)
		"""
		key = str(key)

		if not len(key):
			raise Exception('Field key has zero length')

		self._key = key
		self._value_converter = value_converter or self._Value_Converter or stringConverter
		self._redis = redis or self._Redis

		if self._redis is None:
			raise Exception('Redis client not specified')

	def exists(self):
		"""
		Checking field exists in DB
		"""
		return self._redis.exists(self._key)

	def destroy(self):
		"""
		Removing field from DB
		"""
		return self._redis.delete(self._key)

	def __eq__(self, other):
		return (type(self) == type(other)) and (self._key == other._key)

	def __ne__(self, other):
		return not self.__eq__(other)





class StringField(Field):
	"""
	Represents string fields in DB
	http://redis.io/commands#string
	"""
	def __init__(self, key, value_converter=None, redis=None, overwrite=True):
		"""
		overwrite
			whether overwrite values of existing key by default
		"""
		super(StringField, self).__init__(key, value_converter=value_converter, redis=redis)

		self._overwrite = overwrite

	def set(self, value, overwrite=None):
		"""
		Writes value to field.

		overwrite
			whether overwrite value of existing key, if None, uses default value, defined while instantiating
		"""
		overwrite = self._overwrite if overwrite is None else overwrite
		method = self._redis.set if overwrite else self._redis.setnx

		return method(self._key, self._value_converter.encode(value))

	def get(self, default=None):
		"""
		Gets value from field. If field does not exists, 'default' will be returned
		"""
		data = self._redis.get(self._key)

		return self._value_converter.decode(data) if data is not None else default

	def increment(self, amount=1):
		"""
		Increments numeric value in DB by 'amount'. If field does not exists, "1" will be write in it and return.
		If field contains non numeric value, redis.exceptions.ResponseError will occurred
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
	@classmethod
	def Init(cls, redis, value_converter = None, name_converter = None):
		"""
		As like `Field.Init`, but also initializes default 'name_converter' for all instances of HashField
		"""
		super(HashField, cls).Init(redis=redis, value_converter=value_converter)

		cls._Name_Converter = name_converter


	def __init__(self, key, value_converter=None, name_converter=None, redis=None, overwrite=True):
		"""
		name_converter
			I/O 'name' serializer for current instance

		overwrite
			whether overwrite values of existing name in key by default
		"""
		super(HashField, self).__init__(key, value_converter=value_converter, redis=redis)

		self._name_converter = name_converter or self._Name_Converter or stringConverter

		self._overwrite = overwrite

	def set(self, name, value, overwrite=None):
		"""
		Writes single name-value pair to DB hash field.

		overwrite
			whether overwrite value in existing name in key, if None, uses default value, defined while instantiating
		"""
		overwrite = self._overwrite if overwrite is None else overwrite
		method = self._redis.hset if overwrite else self._redis.hsetnx

		return method(self._key,
			self._name_converter.encode(name),
			self._value_converter.encode(value)
		)

	def set_multi(self, dictionary, overwrite=None):
		"""
		Writes multiple name-value pairs from dictionary to DB hash field.

		overwrite
			whether overwrite value in existing names in key, if None, uses default value, defined while instantiating
		"""
		overwrite = self._overwrite if overwrite is None else overwrite
		method = self._redis.hmset if overwrite else self._redis.hmsetnx

		return method(self._key,
			{self._name_converter.encode(name): self._value_converter.encode(value) for name, value in dictionary.items()}
		)

	def get(self, name, default=None):
		"""
		Gets value from name-value pair in field by 'name'.
		If field or name does not exists, 'default' will be returned
		"""
		value = self._redis.hget(
			self._key, self._name_converter.decode(name)
		)

		return self._value_converter.decode(value) if value is not None else default

	def members(self):
		"""
		Returns all name-value pairs, stored in hash field
		"""
		return {
		self._name_converter.decode(name): self._value_converter.decode(value)
		for name, value in self._redis.hgetall(self._key).items()
		}

	def contains(self, name):
		"""
		Checks whether hash field contains 'name'
		"""
		return self._redis.hexists(self._key, self._name_converter.encode(name))

	def names(self):
		"""
		Return set of names in hash field
		"""
		return {self._name_converter.decode(name) for name in self._redis.hkeys(self._key)}

	def values(self):
		"""
		Return list of value in hash field
		"""
		return [self._value_converter.decode(value) for value in self._redis.hvals(self._key)]

	def delete(self, name):
		"""
		Deletes name-value pair form hash field by 'name'
		"""
		return self._redis.hdel(self._key, self._name_converter.encode(name))

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

	keys = names

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
		"""
		return self._redis.sadd(self._key, *map(self._value_converter.encode, values))

	def delete(self, *values):
		"""
		Removes 'values' from set field
		"""
		return self._redis.srem(self._key, *map(self._value_converter.encode, values))

	def members(self):
		"""
		Gets all members from set field
		"""
		return map(self._value_converter.decode, self._redis.smembers(self._key))

	def count(self):
		"""
		Gets count of members in set field
		"""
		return self._redis.scard(self._key)

	def contains(self, value):
		"""
		Checks whether set field contains 'value'
		"""
		return self._redis.sismember(self._key, self._value_converter.encode(value))

	def random(self, number=None, unique=True):
		"""
		Return random value(s) form set field

		number
			how many values must be returned (1 by default). Must be positive!

		unique
			if True, result list will contains unique values and limited by 'number'
		"""
		if number:
			if number < 0:
				raise Exception('Number must be positive')

			if not unique:
				number *= -1

			return map(self._value_converter.decode, self._redis.srandmember(self._key, number))
		else:
			return self._value_converter.decode(self._redis.srandmember(self._key))

	def pop(self):
		"""
		Returns random value and removes it from set field
		"""
		return self._value_converter.decode(self._redis.spop(self._key))

	def __len__(self):
		return self.count()

	def __iter__(self):
		return self.members().__iter__()

	def __contains__(self, value):
		return self.contains(value)
