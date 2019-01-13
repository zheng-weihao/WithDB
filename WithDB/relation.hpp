#pragma once

#include "definitions.hpp"
#include "utils.hpp"

#include <algorithm>
#include <exception>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace db {
	struct LargeObjectBase {
		address _address;
		address _size;
	public:
		// TOOD: derived class: blob, clob
		virtual string toString() {
			return "LargeObjectBase: address=" + db::toString(_address);
		}
	};

	// continuous tuple for building and retrive on one page or copy the whole tuple to in without reference
	template<typename Relation>
	struct BasicTuple : std::vector<element_t> {
		using Super = std::vector<element_t>;

		Relation &_relation;
		bool _flag; // signal for builder started and tuple update/create mantain by higher layer objects
		
	public:
		inline BasicTuple(Relation &relation, bool build = false) : _relation(relation), _flag(build) {
		}

		inline BasicTuple(const BasicTuple &other) : Super(other), _relation(other._relation), _flag(other._flag) {
		}

		inline BasicTuple(BasicTuple &&other) : Super(std::move(other)), _relation(other._relation), _flag(std::move(other._flag)) {
		}

		inline BasicTuple &operator=(const BasicTuple &other) {
			// check if the relation reference is the same
			if (&_relation != &other._relation) {
				throw std::runtime_error("[BasicTuple::operator=]");
			}
			Super::operator=(other);
			_flag = other._flag;
			return *this;
		}

		inline BasicTuple &operator=(BasicTuple &&other) {
			// check if the relation reference is the same
			if (&_relation != &other._relation) {
				throw std::runtime_error("[BasicTuple::operator=]");
			}
			Super::operator=(std::move(other));
			_flag = std::move(other._flag);
			return *this;
		}

		inline void clear() {
			Super::clear();
			_flag = false;
		}
		
		template<typename Iterator>
		inline BasicTuple &append(Iterator first, Iterator last) {
			insert(end(), first, last);
		}

		template<typename Key, typename Value>
		inline bool read(const Key &key, Value &value) {
			return _relation.read(*this, key, value);
		}

		template<typename Value, typename Key, typename... Args> // for Value declare convenience
		inline Value get(const Key &key, const Args&... args) {
			return _relation.get<Value>(*this, key);
		}

		template<typename Key, typename Value>
		inline bool write(const Key &key, const Value &value) {
			return _relation.write(*this, key, value);
		}
	};

	template<typename Relation>
	struct BasicTupleBuilder {
		using Tuple = BasicTuple<Relation>;

		Relation &_relation;
		Tuple _tuple;
		std::vector<bool> _flags;

	public:
		inline BasicTupleBuilder(Relation &relation, bool start = false) : _relation(relation), _tuple(relation) {
			if (!relation.isFormatted()) {
				throw std::runtime_error("[BasicTupleBuilder::constructor]");
			}
			_flags.resize(relation.attributeSize());
			if (start) {
				this->start();
			}
		}

		inline BasicTupleBuilder(const BasicTupleBuilder &other) : _relation(other._relation), _tuple(other._tuple), _flags(other._tuple) {
		}

		inline BasicTupleBuilder(BasicTupleBuilder &&other) : _relation(other._relation), _tuple(std::move(other._tuple)), _flags(std::move(other._tuple)) {
		}

		inline bool isStarted() {
			return _tuple._flag;
		}

		inline BasicTupleBuilder &start() {
			clear();
			_tuple._flag = true;
			_tuple.resize(_relation.fixedTupleSize());
			return *this;
		}

		inline void clear() {
			if (!isStarted()) {
				return;
			}
			_tuple.clear();
			std::fill(_flags.begin(), _flags.end(), false);
		}

		inline Tuple complete() {
			if (!isStarted()) {
				throw std::runtime_error("[BasicTupleBuilder::complete]");
			}
			// TODO: default value setting by relation defination
			size_t i = 0;
			for (auto f : _flags) {
				if (!f) {
					// attributeDefault return default value in string
					build(i, _relation.attributeDefault(i));
				}
				++i;
			}
			Tuple tuple = std::move(_tuple);
			clear();
			return tuple;
		}

		template<typename Key, typename Value>
		inline BasicTupleBuilder &build(const Key &key, const Value &value) {
			_flags[_relation.attributePos(key)] = _tuple.write(key, value);
			return *this;
		}
	};

	namespace ns::relation {
		template<typename Type>
		using check_string = std::enable_if<std::is_same_v<string, Type>>;
		template<typename Type>
		using check_string_t = typename check_string<Type>::type;

		template<typename Type>
		using check_lob = std::enable_if<std::is_base_of_v<LargeObjectBase, Type>>;
		template<typename Type>
		using check_lob_t = typename check_lob<Type>::type;

		template<typename Type>
		using check_default_t = std::void_t<
			check_not_t<check_lob<Type>>,
			check_not_t<check_string<Type>>
		>;
	}

	struct Attribute {
		string _name;
		type_enum _type;
		page_address _size; // used by CHAR & VARCHAR
		page_address _offset;
		size_t _vCount;

	public:
		inline Attribute(const string &name = "", type_enum type = DUMMY_T, page_address size = 0, page_address offset = 0, size_t vCount = 0)
			: _name(name), _type(type), _size(size), _offset(offset), _vCount(vCount) {
		}

		inline string &name() { return _name; }

		inline page_address fixedSize() {
			switch (_type) {
			case db::CHAR_T:
				return _size;
			case db::VARCHAR_T:
				return 2 * sizeof(page_address);
			case db::INT_T:
				return sizeof(int_t);
			case db::LONG_T:
				return sizeof(long_t);
			case db::FLOAT_T:
				return sizeof(float_t);
			case db::DOUBLE_T:
				return sizeof(double_t);
			case db::DATE_T:
				return _size = sizeof(element_t) * 8;
			case db::BLOB_T:
				return _size = sizeof(address);
			default:
				throw std::runtime_error("[Attribute::fixedSize]");
			}
		}

		inline page_address maxSize() {
			if (_type == VARCHAR_T) {
				return fixedSize() + _size;
			} else {
				return fixedSize();
			}
		}

		// function overload set for attribute reading
		// TODO: add RTTI checking for _type in the future
		// TODO: BLOB => address in tuple, save in a blob object in memory
		// VARCHAR, CHAR, DATE => string
		template<typename Tuple, typename Value
			, ns::relation::check_default_t<Value> * = nullptr
		> inline bool read(Tuple &tuple, Value &value) {
			return BasicTypes::read(value, tuple.data() + _offset, _type);
		}

		template<typename Tuple, typename Value
			, ns::relation::check_string_t<Value> * = nullptr
		> inline bool read(Tuple &tuple, Value &value) {
			if (tuple.size() < _offset + fixedSize()) {
				return false;
			}
			auto ptr = tuple.data();
			switch (_type) {
			case db::CHAR_T:
			case db::DATE_T:
				return BasicTypes::read(value, ptr + _offset, ptr + _offset + _size, _type);
			case db::VARCHAR_T:
			{
				auto b = BasicTypes::get<page_address>(ptr + _offset);
				auto e = BasicTypes::get<page_address>(ptr + _offset + sizeof(page_address));
				if (e < b || _size < e - b || tuple.size() < e 
					|| !BasicTypes::read(value, ptr + b, ptr + e, _type)) {
					return false;
				}
				break;
			}
			case db::INT_T:
				value = std::move(toString(get<int_t>(tuple))); break;
			case db::LONG_T:
				value = std::move(toString(get<long_t>(tuple))); break;
			case db::FLOAT_T:
				value = std::move(toString(get<float_t>(tuple))); break;
			case db::DOUBLE_T:
				value = std::move(toString(get<double_t>(tuple))); break;
			default:
				return false;;
			}
			return true;
		}

		template<typename Tuple, typename Value
			, ns::relation::check_lob_t<Value> * = nullptr
		> inline bool read(Tuple &tuple, Value &value) {
			if (_type < LOB_T || _type > CLOB_T || tuple.size() < _offset + sizeof(address)) {
				return false;
			}
			return BasicTypes::read<address>(value._address, tuple.data() + _offset);
		}

		template<typename Value, typename Tuple, typename... Args> // for Value declare convenience
		inline Value get(Tuple &tuple, const Args &... args) {
			Value v = getInstance<Value>(args...);
			if (read(tuple, v)) {
				return v;
			}
			throw std::runtime_error("[Attribute::get]");
		}

		template<typename Tuple, typename Value
			, ns::relation::check_default_t<Value> * = nullptr
		> inline bool write(Tuple &tuple, const Value &value) {
			return BasicTypes::write(value, tuple.data() + _offset, _type);
		}

		template<typename Tuple, typename Value
			, ns::relation::check_string_t<Value> * = nullptr
		> inline bool write(Tuple &tuple, const Value &value) {
			if (tuple.size() < _offset + fixedSize()) {
				return false;
			}
			auto ptr = tuple.data();
			try {
				switch (_type) {
				case db::CHAR_T:
				case db::DATE_T:
					return BasicTypes::write(value, ptr + _offset, ptr + _offset + _size, _type);
				case db::VARCHAR_T:
				{
					auto size = value.size();
					if (_size < size) {
						return false;
					}
					auto b = BasicTypes::get<page_address>(ptr + _offset);
					auto e = BasicTypes::get<page_address>(ptr + _offset + sizeof(page_address));
					if (e) {
						if (e < b || _size < e - b || tuple.size() < e) {
							return false;
						}
						tuple.erase(tuple.begin() + b, tuple.begin() + e);
					}
					b = static_cast<page_address>(tuple.size());
					e = static_cast<page_address>(b + size);
					tuple.resize(e);
					ptr = tuple.data();
					return BasicTypes::write(b, ptr + _offset)
						&& BasicTypes::write(e, ptr + _offset + sizeof(page_address))
						&& BasicTypes::write(value, ptr + b, ptr + e, _type);
				}
				case db::INT_T:
					return write(tuple, std::stoi(value));
				case db::LONG_T:
					return write(tuple, std::stoll(value));
				case db::FLOAT_T:
					return write(tuple, std::stof(value));
				case db::DOUBLE_T:
					return write(tuple, std::stod(value));
				default:
					return false;;
				}
			} catch (...) {
				return false;
			}
		}

		template<typename Tuple, typename Value
			, ns::relation::check_lob_t<Value> * = nullptr
		> inline bool write(Tuple &tuple, const Value &value) {
			if (_type < LOB_T || _type > CLOB_T || tuple.size() < _offset + sizeof(address)) {
				return false;
			}
			return BasicTypes::write<address>(value._address, tuple.data() + _offset);
		}
	};

	struct Relation {
		using TupleBuilder = BasicTupleBuilder<Relation>;

		string _name;
		address _capacity; // relation max size
		address _begin; // relation begin pointer
		address _end; // current end pointer
		address _ptr; // current search pointer for insert

		std::vector<Attribute> _attributes; // _fixedSize, _maxSize
		std::unordered_map<string, size_t> _attributeNames; // attribute name-index map // TODO: might remove
		bool _isFormatted;

		address _fixedTupleSize = 0;
		address _maxTupleSize = 0;

		// statistic
		size_t _tCount = 0; // tuple count
		size_t _pCount = 0; // page count

		std::vector<bool> _flags; // used as indexes on single attribute for regular relation, used as masks for temporary relation in project

		string _stringDefault = "";
		string _arithmeticDefault = "0";

		inline Relation(const string &name, address capacity = 0, address begin = 0, bool load = false)
			: _name(name), _capacity(capacity), _begin(begin), _end(begin), _ptr(begin), _isFormatted(load) {
		}

		inline explicit Relation(bool load = false) : Relation("", 0, 0, load) {
		}

		inline string &name() { return _name; }

		inline bool isFormatted() {
			return _isFormatted;
		}

		inline Relation &format(bool load = false) {
			if (isFormatted() != load) {
				throw std::runtime_error("[Relation::format]");
			}

			_flags.resize(_attributes.size());

			if (load) {
				return *this;
			}

			for (auto &attribute : _attributes) {
				_fixedTupleSize += attribute.fixedSize();
				_maxTupleSize += attribute.maxSize();
				if (_maxTupleSize > TUPLE_CAPACITY) {
					// size overflow
					throw std::runtime_error("[Relation::format]");
				}
			}

			std::vector<size_t> order(_attributes.size());
			for (size_t i = 0; i != order.size(); ++i) {
				order[i] = i;
			}
			std::sort(order.begin(), order.end(), [this](size_t ai, size_t bi) {
				auto a = _attributes[ai], &b = _attributes[bi];
				if ((a._type == CHAR_T) ^ (b._type == CHAR_T)) {
					return b._type == CHAR_T;
				}
				auto as = a.fixedSize(), bs = b.fixedSize();
				if (as != bs) {
					return bs < as;
				} else {
					return ai < bi;
				}
			});

			page_address offset = 0;
			for (auto &index : order) {
				auto &attribute = _attributes[index];
				attribute._offset = offset;
				offset += attribute.fixedSize();

				// for attribute storage consistency
				if (attribute._type != CHAR_T && attribute._type != VARCHAR_T) {
					attribute._size = attribute.fixedSize();
				}
			}
			_isFormatted = true;
			return *this;
		}

		inline address fixedTupleSize() {
			return _fixedTupleSize;
		}

		inline address maxTupleSize() {
			return _maxTupleSize;
		}

		template<typename... Args>
		inline Relation &addAttribute(const Args &... args) {
			if (isFormatted()) {
				throw std::runtime_error("[Relation::addAttribute]");
			}
			Attribute a(args...);
			auto &name = a.name();
			if (_attributeNames.insert({ name, _attributes.size() }).second) {
				_attributes.push_back(std::move(a));
			}
			return *this;
		}

		template<typename... Args>
		inline Relation &loadAttribute(size_t pos, const Args &... args) {
			Attribute a(args...);
			if (!isFormatted() 
				|| (pos < _attributes.size() && _attributes[pos]._type != DUMMY_T) 
				|| !_attributeNames.try_emplace(a.name(), pos).second) {
				throw std::runtime_error("[Relation::loadAttribute]");
			}
			if (pos >= _attributes.size()) {
				_attributes.resize(pos + 1);
			}
			_attributes[pos] = std::move(a);
			return *this;
		}

		inline size_t attributeSize() {
			return _attributes.size();
		}
		
		inline size_t attributePos(size_t pos) {
			return std::min(pos, _attributes.size());
		}

		inline size_t attributePos(const string &name) {
			auto iter = _attributeNames.find(name);
			return iter == _attributeNames.end() ? _attributes.size() : iter->second;
		}

		inline const string &attributeName(size_t pos) {
			if (pos < _attributes.size()) {
				return _attributes[pos].name();
			} else {
				return _stringDefault;
			}
		}

		inline const string &attributeName(const string &name) {
			auto iter = _attributeNames.find(name);
			return iter == _attributeNames.end() ? _stringDefault : iter->first;
		}

		template<typename Key>
		inline Attribute &attribute(const Key &key) {
			auto pos = attributePos(key);
			if (pos < _attributes.size()) {
				return _attributes[pos];
			}
			throw std::runtime_error("[Relation::attribute]");
		}

		// return string const reference
		template<typename Key>
		inline const string &attributeDefault(const Key &key) {
			// TODO: attributeDefault decide by attribute setting
			switch (attribute(key)._type) {
			case db::INT_T:
			case db::LONG_T:
			case db::FLOAT_T:
			case db::DOUBLE_T:
				return _arithmeticDefault;
			default:
				return _stringDefault;
			}
		}

		template<typename Tuple, typename Key, typename Value>
		inline bool read(Tuple &tuple, const Key &key, Value &value) {
			return attribute(key).read(tuple, value);
		}

		template<typename Value, typename Tuple, typename Key, typename... Args> // for Value declare convenience
		inline Value get(Tuple &tuple, const Key &key, const Args&... args) {
			return attribute(key).get<Value>(tuple, args...);
		}

		template<typename Tuple, typename Key, typename Value>
		inline bool write(Tuple &tuple, const Key &key, const Value &value) {
			return attribute(key).write(tuple, value);
		}

		inline TupleBuilder builder(bool start = false) {
			return TupleBuilder(*this, start);
		}
	};

	using TupleBuilder = typename Relation::TupleBuilder;

	using Tuple = typename TupleBuilder::Tuple;

	// assuming positions (relation and attribute) are in uint32 range
	struct Schema {
		constexpr static size_t META_CAPACITY = SEGMENT_SIZE / 4;
		constexpr static size_t RELATION_META_POS = 0;
		constexpr static size_t ATTRIBUTE_META_POS = 1;
		constexpr static size_t INDEX_META_POS = 2;

		constexpr static size_t getIndexKey(size_t rpos, size_t apos) {
			return (rpos << 32) | apos;
		}

		constexpr static size_t getRelationFromKey(size_t key) {
			return getFlag(key, 32, 64);
		}
		constexpr static size_t getAttributeFromKey(size_t key) {
			return getFlag(key, 0, 32);
		}

		std::vector<Relation *> _relations; // _fixedSize, _maxSize
		std::unordered_map<string, size_t> _relationNames; // attribute name-index map // TODO: might remove
		std::unordered_map<size_t, address> _indexes;

		inline Schema() {
			{
				Relation meta("RelationMeta", META_CAPACITY, (RELATION_META_POS + 1) * META_CAPACITY);
				meta.addAttribute("Name", db::VARCHAR_T, 255)
					.addAttribute("Position", db::INT_T)
					.addAttribute("Capacity", db::LONG_T)
					.addAttribute("Begin", db::LONG_T)
					.addAttribute("End", db::LONG_T)
					.addAttribute("Pointer", db::LONG_T)
					.addAttribute("FixedTupleSize", db::INT_T)
					.addAttribute("MaxTupleSize", db::INT_T)
					.addAttribute("TupleCount", db::LONG_T)
					.addAttribute("PageCount", db::LONG_T)
					.format();
				createRelation(std::move(meta), RELATION_META_POS);
			}
			{
				Relation meta("AttributeMeta", META_CAPACITY, (ATTRIBUTE_META_POS + 1) * META_CAPACITY);
				meta.addAttribute("RelationPosition", db::INT_T)
					.addAttribute("Name", db::VARCHAR_T, 255)
					.addAttribute("Position", db::INT_T)
					.addAttribute("Type", db::INT_T)
					.addAttribute("Size", db::INT_T)
					.addAttribute("Offset", db::INT_T)
					.addAttribute("ValueCount", db::LONG_T)
					.format();
				createRelation(std::move(meta), ATTRIBUTE_META_POS);
			}
			{
				Relation meta("IndexMeta", META_CAPACITY, (INDEX_META_POS + 1) * META_CAPACITY);
				meta.addAttribute("RelationPosition", db::INT_T)
					.addAttribute("AttributePosition", db::INT_T)
					.addAttribute("Root", db::LONG_T)
					.format();
				createRelation(std::move(meta), INDEX_META_POS);
			}
		}

		inline ~Schema() {
			for (auto &ptr : _relations) {
				delete ptr;
			}
		}

		inline size_t relationPos(size_t pos) {
			if (pos < _relations.size() && _relations[pos]) {
				return pos;
			} else {
				return _relations.size();
			}
		}

		inline size_t relationPos(const string &name) {
			auto iter = _relationNames.find(name);
			return relationPos(iter == _relationNames.end() ? _relations.size() : iter->second);
		}

		inline const string &relationName(size_t pos) {
			if (pos < _relations.size() && _relations[pos]) {
				return _relations[pos]->name();
			} else {
				return _relations[RELATION_META_POS]->_stringDefault;
			}
		}

		inline  const string &relationName(const string &name) {
			auto iter = _relationNames.find(name);
			return iter == _relationNames.end() ? _relations[RELATION_META_POS]->_stringDefault : iter->first;
		}

		template<typename Key>
		inline Relation &relation(const Key &key) {
			auto pos = relationPos(key);
			if (pos != _relations.size()) {
				return *_relations[pos];
			}
			throw std::runtime_error("[Relation::relation]");
		}

		inline bool loadRelation(Tuple &tuple) {
			auto name = tuple.get<string>(0);
			size_t pos = static_cast<std::uint32_t>(tuple.get<int_t>(1)); // TODO: turn unsign function
			if ((pos < _relations.size() && _relations[pos]) || !_relationNames.try_emplace(name, pos).second) {
				return false;
			}
			if (pos >= _relations.size()) {
				_relations.resize(pos + 1);
			}
			_relations[pos] = new Relation(true);
			auto &r = *_relations[pos];
			r._name = name;
			r._capacity = tuple.get<long_t>(2);
			r._begin = tuple.get<long_t>(3);
			r._end = tuple.get<long_t>(4);
			r._ptr = tuple.get<long_t>(5);
			r._fixedTupleSize = static_cast<std::uint32_t>(tuple.get<int_t>(6));
			r._maxTupleSize = static_cast<std::uint32_t>(tuple.get<int_t>(7));
			r._tCount = tuple.get<long_t>(8);
			r._pCount = tuple.get<long_t>(9);
			return true;
		}

		template<typename Key>
		inline Tuple dumpRelation(const Key &key) {
			auto pos = relationPos(key);
			if (pos == _relations.size()) {
				throw std::runtime_error("[Schema::dumpRelation]");
			}
			auto &r = relation(pos);
			return relation(RELATION_META_POS).builder(true)
				.build(0, r.name())
				.build(1, static_cast<int_t>(pos))
				.build(2, static_cast<long_t>(r._capacity))
				.build(3, static_cast<long_t>(r._begin))
				.build(4, static_cast<long_t>(r._end))
				.build(5, static_cast<long_t>(r._ptr))
				.build(6, static_cast<int_t>(r._fixedTupleSize))
				.build(7, static_cast<int_t>(r._maxTupleSize))
				.build(8, static_cast<long_t>(r._tCount))
				.build(9, static_cast<long_t>(r._pCount))
				.complete();
		}

		inline bool loadAttribute(Tuple &tuple) {
			size_t rpos = static_cast<std::uint32_t>(tuple.get<int_t>(0));
			if (relationPos(rpos) == _relations.size()) {
				return false;
			}

			auto &r = *_relations[rpos];
			try {
				r.loadAttribute(
					static_cast<std::uint32_t>(tuple.get<int_t>(2)),
					tuple.get<string>(1),
					static_cast<type_enum>(tuple.get<int_t>(3)),
					static_cast<page_address>(tuple.get<int_t>(4)),
					static_cast<page_address>(tuple.get<int_t>(5)),
					tuple.get<long_t>(6)
				);
			} catch (...) {
				return false;
			}
			return true;
		}

		template<typename RelationKey, typename AttributeKey>
		inline Tuple dumpAttribute(const RelationKey &rkey, const AttributeKey &akey) {
			auto rpos = relationPos(rkey);
			if (rpos == _relations.size()) {
				throw std::runtime_error("[Schema::dumpRelation]");
			}
			auto &r = relation(rpos);
			auto apos = r.attributePos(akey);
			if (apos == r._attributes.size()) {
				throw std::runtime_error("[Schema::dumpRelation]");
			}
			auto &a = r.attribute(apos);
			return relation(ATTRIBUTE_META_POS).builder(true)
				.build(0, static_cast<int_t>(rpos))
				.build(1, static_cast<string>(a.name()))
				.build(2, static_cast<int_t>(apos))
				.build(3, static_cast<int_t>(a._type))
				.build(4, static_cast<int_t>(a._size))
				.build(5, static_cast<int_t>(a._offset))
				.build(6, static_cast<long_t>(a._vCount))
				.complete();
		}

		inline size_t createRelation(Relation &&relation, size_t pos) {
			if (!relation.isFormatted() || _relationNames.find(relation.name()) != _relationNames.end() || (pos < _relations.size() && _relations[pos])) {
				return false;
			}
			if (_relationNames.try_emplace(relation.name(), pos).second) {
				if (pos >= _relations.size()) {
					_relations.resize(pos);
					_relations.emplace_back(new Relation(std::move(relation)));
				} else {
					_relations[pos] = new Relation(std::move(relation));
				}
				return true;
			} else {
				return false;
			}
		}

		template<typename Key>
		inline bool dropRelation(const Key &key) {
			auto pos = relationPos(key);
			if (pos < _relations.size() && _relations[pos]) {
				_relationNames.erase(relation(pos).name());
				delete _relations[pos];
				_relations[pos] = nullptr;
			} else {
				return false;
			}
		}

		inline bool loadIndex(Tuple &tuple) {
			size_t rpos = static_cast<std::uint32_t>(tuple.get<int_t>(0));
			if (relationPos(rpos) == _relations.size()) {
				return false;
			}
			auto &r = *_relations[rpos];
			size_t apos = static_cast<std::uint32_t>(tuple.get<int_t>(1));
			if (r.attributePos(apos) == r._attributes.size()) {
				return false;
			}
			address root = tuple.get<long_t>(2);
			if (!_indexes.try_emplace(getIndexKey(rpos, apos), root).second) {
				return false;
			}
			r._flags[apos] = true;
			return true;
		}

		inline Tuple dumpIndex(size_t key) {
			auto iter = _indexes.find(key);
			if (iter != _indexes.end()) {
				throw std::runtime_error("[Schema::dumpIndex]");
			}
			return relation(INDEX_META_POS).builder(true)
				.build(0, static_cast<int_t>(getRelationFromKey(key)))
				.build(1, static_cast<int_t>(getAttributeFromKey(key)))
				.build(2, static_cast<long_t>(iter->second))
				.complete();
		}

		template<typename RelationKey, typename AttributeKey>
		inline Tuple dumpIndex(const RelationKey &rkey, const AttributeKey &akey) {
			size_t rpos = relationPos(rkey);
			if (rpos == _relations.size()) {
				throw std::runtime_error("[Schema::dumpIndex]");
			}
			auto &r = *_relations[rpos];
			size_t apos = r.attributePos(akey);
			if (apos == r._attributes.size() || !r._flags[apos]) {
				throw std::runtime_error("[Schema::dumpIndex]");
			}
			return dumpIndex(getIndexKey(rpos, apos));
		}

		template<typename RelationKey, typename AttributeKey>
		inline address index(const RelationKey &rkey, const AttributeKey &akey) { // check index status
			size_t rpos = relationPos(rkey);
			if (rpos == _relations.size()) {
				throw std::runtime_error("[Schema::dumpIndex]");
			}
			auto &r = *_relations[rpos];
			size_t apos = r.attributePos(akey);
			if (apos == r._attributes.size() || !r._flags[apos]) {
				throw std::runtime_error("[Schema::dumpIndex]");
			}
			auto key = getIndexKey(relationPos(rkey), attributePos(akey));
			auto iter = _indexes.find(key);
			if (iter != _indexes.end()) {
				return iter->second;
			} else {
				return NULL_ADDRESS;
			}
		}

		template<typename RelationKey, typename AttributeKey>
		inline bool setIndex(const RelationKey &rkey, const AttributeKey &akey, address root = NULL_ADDRESS) { // root = NULL_ADDRESS => delete index
			auto key = getIndexKey(relationPos(rkey), attributePos(akey));
			auto iter = _indexes.find(key);
			if ((iter == _indexes.end()) ^ (root != NULL_ADDRESS)) {
				return false;
			} else {
				return NULL_ADDRESS;
			}
			if (root == NULL_ADDRESS) {
				_indexes.erase(iter);
			} else {
				_indexes.try_emplace(key, root);
			}
			return true;
		}
	};

}
