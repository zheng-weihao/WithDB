#pragma once

#include "definitions.hpp"
#include "endian.hpp"
#include "page.hpp"

#include <string>
#include <unordered_map>

namespace db {
	using TupleContainer = NaiveContainer<0xffff>; // TODO: legacy temp structure
	// continuous tuple for building and retrive on one page or copy the whole tuple to in without reference
	using Tuple = typename TupleContainer::page_type; // TODO: Tuple should be a page<0xffff> with self arrange space(self hold container)

	struct AttributeEntry {
		std::string _name; // to save space in relation attribute table
		attribute_enum _type;
		page_address _size;
		size_t _index;
		page_address _offset;
		size_t _vCount;

	public:
		inline AttributeEntry(const std::string &name, attribute_enum type, page_address size = 0, size_t index = 0, page_address offset = 0, size_t vCount = 0) : _name(name), _index(index), _type(type), _size(size), _offset(offset), _vCount(vCount) {
		}

		inline page_address fixedSize() {
			switch (_type) {
			case db::CHAR_T:
				return _size;
			case db::VARCHAR_T:
				return 2 * sizeof(page_address);
			case db::INT_T:
				return _size = sizeof(int_type);
			case db::LONG_T:
				return _size = sizeof(long_type);
			case db::FLOAT_T:
				return _size = sizeof(float_type);
			case db::DOUBLE_T:
				return _size = sizeof(double_type);
			case db::DATE_T:
				return _size = sizeof(element_type) * 8;
			case db::BLOB_T:
				return _size = sizeof(address);
			default:
				throw std::runtime_error("[AttributeEntry::fixedSize]");
			}
		}

		inline page_address maxSize() {
			if (_type == VARCHAR_T) {
				return _size + 2 * sizeof(page_address);
			} else {
				return _size;
			}
		}

		// BLOB => address
		// VARCHAR, CHAR, DATE => string
		template<typename Type>
		inline Type read(Tuple &tuple) {
			if (_size != sizeof(Type)) {
				// simple type size checking without reflection for efficiency
				// TODO: add RTTI checking for _type in the future
				throw std::runtime_error("[AttributeEntry::read]");
			}
			return tuple.read<Type>(_offset);
		}

		template<>
		inline std::string read<std::string>(Tuple &tuple) {
			// TODO: checking condition for robustness
			switch (_type) {
			case db::CHAR_T:
			case db::DATE_T:
				return tuple.read<std::string>(_offset, _offset + _size);
			case db::VARCHAR_T:
			{
				auto b = tuple.read<page_address>(_offset);
				auto e = tuple.read<page_address>(_offset + sizeof(page_address));
				if (e - b > _size) {
					throw std::runtime_error("[AttributeEntry::read]");
				}
				return tuple.read<std::string>(b, e);
			}
			default:
				throw std::runtime_error("[AttributeEntry::read]");
			}
		}
	};

	struct Relation {
		std::string _name;

		address _capacity = 0; // relation max size
		address _begin = 0; // relation begin pointer
		address _end = 0; // current end pointer
		address _ptr = 0; // current search pointer for insert

		std::vector<AttributeEntry> _attributes; // _fixedSize, _maxSize
		std::unordered_map<std::string, size_t> _map; // attribute name-index map // TODO: might remove

		address _fixedTupleSize = 0;
		address _maxTupleSize = 0;

		// TODO: IndexTable _indexes;

		// statistic
		size_t _tCount = 0;
		size_t _bCount = 0; // TODO: change to _pCount

		inline Relation(const std::string &name = "") : _name(name) {
		}

		inline std::string &name() {
			return _name;
		}

		template<typename... Args>
		inline Relation &add(Args... args) {
			if (isFormatted()) {
				throw std::runtime_error("[Relation::add]");
			}
			_attributes.emplace_back(args...);
			return *this;
		}

		inline bool isFormatted() {
			return !_map.empty();
		}

		inline bool format() {
			if (isFormatted()) {
				return true;
			}
			if (_attributes.empty()) {
				return false;
			}

			if (_fixedTupleSize) { // as a symbol that relation is set from storage
				std::sort(_attributes.begin(), _attributes.end(), [](const AttributeEntry &a, const AttributeEntry &b) {
					return a._index < b._index;
				});
				size_t i = 0;
				for (auto &entry : _attributes) {
					if (entry._index != i || !_map.insert(std::make_pair(entry._name, i)).second) {
						// index miss or duplicated attribute name
						return false;
					}
					++i;
				}
				return true;
			}

			std::vector<size_t> order(_attributes.size());
			size_t i = 0;
			for (auto &entry : _attributes) {
				_fixedTupleSize += entry.fixedSize();
				_maxTupleSize += entry.maxSize();
				if (_maxTupleSize > TUPLE_CAPACITY) {
					// size overflow
					return false;
				}
				if (!_map.insert(std::make_pair(entry._name, i)).second) {
					// duplicated attribute name
					return false;
				}
				entry._index = i;
				order[i] = i;
				++i;
			}
			std::sort(order.begin(), order.end(), [this](size_t a, size_t b) {
				auto aEntry = _attributes[a];
				auto bEntry = _attributes[b];
				if ((aEntry._type == CHAR_T) ^ (bEntry._type == CHAR_T)) {
					return bEntry._type == CHAR_T;
				} else {
					return aEntry.fixedSize() > bEntry.fixedSize();
				}
			});
			page_address b = 0;
			for (auto &index : order) {
				auto &entry = _attributes[index];
				entry._offset = b;
				b += entry.fixedSize();
			}
			return true;
		}

		inline address fixedTupleSize() {
			return _fixedTupleSize;
		}

		inline address maxTupleSize() {
			return _maxTupleSize;
		}

		template<typename Type>
		inline Type read(Tuple &tuple, size_t index) {
			return _attributes[index].read<Type>(tuple);
		}

		template<typename Type>
		inline Type read(Tuple &tuple, const std::string name) {
			return read<Type>(tuple, _map[name]);
		}

		struct TupleBuilder {
			Relation &_relation;
			std::unique_ptr<Tuple> _current;
			std::vector<bool> _flags;
		public:
			inline TupleBuilder(Relation &relation) : _relation(relation), _flags(_relation._attributes.size(), false) {
			}

			inline bool isStarted() {
				return _current.operator bool();
			}

			inline TupleBuilder &start(TupleContainer &container, size_t offset = 0) {
				if (container.size() < offset + _relation.maxTupleSize()) {
					throw std::runtime_error("[TupleBuilder::start]");
				}
				clear();
				_current = std::make_unique<Tuple>(container, offset, offset + _relation.fixedTupleSize());
				return *this;
			}

			inline Tuple result() {
				if (!isStarted()) {
					throw std::runtime_error("[TupleBuilder]");
				}

				size_t i = 0;
				for (auto f : _flags) {
					if (!f) {
						auto t = _relation._attributes[i]._type;
						if (t == CHAR_T || t == VARCHAR_T || t == DATE_T) { // TODO: default value
							build(std::string(), i);
						} else {
							build(0, i);
						}
					}
					++i;
				}
				Tuple tuple = std::move(*_current);
				clear();
				return std::move(tuple);
			}

			inline void clear() {
				if (isStarted()) {
					_current.reset();
					std::fill(_flags.begin(), _flags.end(), false);
				}
			}

			template<typename Type>
			inline TupleBuilder &build(Type value, size_t index) {
				auto &entry = _relation._attributes[index];
				if (_flags[index] && entry._type != VARCHAR_T) { // TODO: current don't support
					throw std::runtime_error("[TupleBuilder::build]");
				}

				write(*_current, entry, value);
				_flags[index] = true;
				return *this;
			}

			template<typename Type>
			inline TupleBuilder &build(Type value, const std::string &name) {
				return build(value, _map[name]);
			}

			// TODO: move place
			template<typename Type>
			inline static void write(Tuple &tuple, AttributeEntry &entry, Type value) {
				if (entry._size != sizeof(Type)) {
					// simple type size checking without reflection for efficiency
					// TODO: maybe add RTTI checking in the future
					throw std::runtime_error("[TupleBuilder::write]");
				}
				tuple.write(value, entry._offset);
			}

			inline static void write(Tuple &tuple, AttributeEntry &entry, const std::string &value) {
				if (entry._size < value.size()) {
					throw std::runtime_error("[TupleBuilder::write]");
				}
				switch (entry._type) {
				case db::CHAR_T:
				case db::DATE_T:
					tuple.write(value, entry._offset, entry._offset + entry._size);
					return;
				case db::VARCHAR_T:
				{
					auto first = static_cast<page_address>(tuple.size());
					auto last = static_cast<page_address>(tuple.size() + value.size());
					tuple.write(first, entry._offset);
					tuple.write(last, entry._offset + sizeof(page_address));
					tuple.activate(tuple.begin(), tuple.end() + value.size());
					tuple.write(value, first);
					return;
				}
				default:
					throw std::runtime_error("[TupleBuilder::write]");
				}
			}
		};

		inline TupleBuilder builder() {
			if (!isFormatted()) {
				throw std::runtime_error("[TupleBuilder::constructor]");
			}
			return TupleBuilder(*this);
		}
	};

	using TupleBuilder = Relation::TupleBuilder;
}
