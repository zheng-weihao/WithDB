#pragma once

#include "keeper.hpp"
#include "relation.hpp"
#include "relation_guard.hpp"
#include "index_guard.hpp"

#include <iostream>
#include <string>
#include <sstream>
#include <map>

namespace db {
	struct MetadataGuard {
		constexpr static size_t BLOCK_CAPACITY = SEGMENT_SIZE / 4;
		constexpr static size_t RELATION_META_INDEX = 1;
		constexpr static size_t ATTRIBUTE_META_INDEX = 2;
		constexpr static size_t INDEX_META_INDEX = 3;

		Keeper &_keeper;
		std::unordered_map<std::string, Relation> _relations;
		std::map<std::pair<std::string, size_t>, address> _indexes;
		RelationGuard _relationMeta;
		RelationGuard _attributeMeta;
		RelationGuard _indexMeta;

	public:
		inline MetadataGuard(Keeper &keeper) : _keeper(keeper), _relations{
			{"RelationMeta", Relation("RelationMeta") },
			{"AttributeMeta", Relation("AttributeMeta") },
			{"IndexMeta", Relation("IndexMeta") },
		}, _relationMeta(_keeper, _relations["RelationMeta"])
		, _attributeMeta(_keeper, _relations["AttributeMeta"])
		, _indexMeta(_keeper, _relations["IndexMeta"]) {
		}

		inline void load() {
			// TODO: config trush code
			auto &r = _relationMeta._relation;
			r.add("Name", db::VARCHAR_T, 255)
				.add("Capacity", db::LONG_T)
				.add("Begin", db::LONG_T)
				.add("End", db::LONG_T)
				.add("Ptr", db::LONG_T)
				.add("FixedTupleSize", db::LONG_T)
				.add("MaxTupleSize", db::LONG_T)
				.add("TCount", db::LONG_T)
				.add("BCount", db::LONG_T)
				.format();
			auto &a = _attributeMeta._relation;
			a.add("RelationName", db::VARCHAR_T, 255)
				.add("Name", db::VARCHAR_T, 255)
				.add("Type", db::INT_T)
				.add("Size", db::INT_T)
				.add("Index", db::LONG_T)
				.add("Offset", db::INT_T)
				.add("VCount", db::LONG_T)
				.format();
			auto &i = _indexMeta._relation;
			i.add("RelationName", db::VARCHAR_T, 255)
				.add("Index", db::LONG_T)
				.add("Ptr", db::LONG_T)
				.format();
			r._capacity = BLOCK_CAPACITY;
			r._ptr = r._begin = RELATION_META_INDEX * BLOCK_CAPACITY;
			a._capacity = BLOCK_CAPACITY;
			a._ptr = a._begin = ATTRIBUTE_META_INDEX * BLOCK_CAPACITY;
			i._capacity = BLOCK_CAPACITY;
			i._ptr = i._begin = INDEX_META_INDEX * BLOCK_CAPACITY;
			auto p = _keeper.hold<VirtualPage>(0);
			r._end = p->read<address>(0);
			r._tCount = p->read<size_t>(sizeof(address));
			r._bCount = p->read<size_t>(sizeof(address) + sizeof(size_t));
			a._end = p->read<address>(sizeof(address) + 2 * sizeof(size_t));
			a._tCount = p->read<size_t>(2 * sizeof(address) + 2 * sizeof(size_t));
			a._bCount = p->read<size_t>(2 * sizeof(address) + 3 * sizeof(size_t));
			i._end = p->read<address>(2 * sizeof(address) + 4 * sizeof(size_t));
			i._tCount = p->read<size_t>(3 * sizeof(address) + 4 * sizeof(size_t));
			i._bCount = p->read<size_t>(3 * sizeof(address) + 5 * sizeof(size_t));
			p->unpin();
			p.reset();
			if (!a._end) {
				_keeper._translator._params[0].first = 1;
				r._end = r._begin;
				a._end = a._begin;
				i._end = i._begin;
			}
			_relationMeta.traverseTuple([this](Tuple &tuple, address addr) {
				Relation relation = toRelation(tuple);
				addRelation(relation, false);
			});
			_attributeMeta.traverseTuple([this](Tuple &tuple, address addr) {
				AttributeEntry entry = toAttribute(tuple);
				addAttribute(entry, _attributeMeta._relation.read<std::string>(tuple, 0));
			});
			_indexMeta.traverseTuple([this](Tuple &tuple, address addr) {
				std::pair<std::string, size_t> p;
				auto ptr = toIndex(tuple, p.first, p.second);
				addIndex(p.first, p.second, ptr);
			});
			for (auto &p : _relations) {
				p.second.format();
			}
		}

		inline void dump() {
			_relationMeta.traverseTuple([this](Tuple &tuple, address addr) {
				auto relation = toRelation(tuple);
				Relation current = _relations[relation._name];
				tuple = toTuple(current, tuple._container);
				_relationMeta.reallocate(addr, tuple);
			});
			_attributeMeta.traverseTuple([this](Tuple &tuple, address addr) {
				auto name = _attributeMeta._relation.read<std::string>(tuple, 0);
				auto entry = toAttribute(tuple);
				auto &relation = _relations[name];
				AttributeEntry current = relation._attributes[relation._map[entry._name]];
				tuple = toTuple(current, name, tuple._container);
				_relationMeta.reallocate(addr, tuple);
			});
			_indexMeta.traverseTuple([this](Tuple &tuple, address addr) {
				std::pair<std::string, size_t> p;
				auto ptr = toIndex(tuple, p.first, p.second);
				ptr = _indexes[p];
				tuple = toTuple(p.first, p.second, ptr, tuple._container);
				_relationMeta.reallocate(addr, tuple);
			});
			auto p = _keeper.hold<VirtualPage>(0);
			auto &r = _relationMeta._relation;
			p->write(r._end, 0);
			p->write(r._tCount, sizeof(address));
			p->write(r._bCount, sizeof(address) + sizeof(size_t));
			auto &a = _attributeMeta._relation;
			p->write(a._end, sizeof(address) + 2 * sizeof(size_t));
			p->write(a._tCount, 2 * sizeof(address) + 2 * sizeof(size_t));
			p->write(a._bCount, 2 * sizeof(address) + 3 * sizeof(size_t));
			auto &i = _indexMeta._relation;
			p->write(i._end, 2 * sizeof(address) + 4 * sizeof(size_t));
			p->write(i._tCount, 3 * sizeof(address) + 4 * sizeof(size_t));
			p->write(i._bCount, 3 * sizeof(address) + 5 * sizeof(size_t));
			p->dump();
		}

		inline Relation toRelation(Tuple &tuple) {
			Relation &relation = _relationMeta._relation;
			Relation ret(relation.read<std::string>(tuple, 0));
			ret._capacity = relation.read<address>(tuple, 1);
			ret._begin = relation.read<address>(tuple, 2);
			ret._end = relation.read<address>(tuple, 3);
			ret._ptr = relation.read<address>(tuple, 4);
			ret._fixedTupleSize = relation.read<address>(tuple, 5);
			ret._maxTupleSize = relation.read<address>(tuple, 6);
			ret._tCount = relation.read<size_t>(tuple, 7);
			ret._bCount = relation.read<size_t>(tuple, 8);
			return std::move(ret);
		}
		
		inline Tuple toTuple(const Relation &relation, TupleContainer &container) {
			container.resize(_relationMeta._relation.maxTupleSize());
			return _relationMeta._relation.builder().start(container)
				.build(relation._name, 0)
				.build(relation._capacity, 1)
				.build(relation._begin, 2)
				.build(relation._end, 3)
				.build(relation._ptr, 4)
				.build(relation._fixedTupleSize, 5)
				.build(relation._maxTupleSize, 6)
				.build(relation._tCount, 7)
				.build(relation._bCount, 8)
				.result();
		}
		
		inline bool addRelation(Relation &relation, bool create = true) {
			if (!create) {
				_relations[relation._name] = relation;
				return true;
			}
			if (!relation.isFormatted() || !_relations.insert(std::make_pair(relation._name, relation)).second) {
				return false;
			}
			TupleContainer tmp(0);
			{
				Tuple t = toTuple(relation, tmp);
				_relationMeta.allocate(t);
			}
			for (auto &entry : relation._attributes) {
				Tuple t = toTuple(entry, relation._name, tmp);
				_attributeMeta.allocate(t);
			}
			return true;
		}

		inline AttributeEntry toAttribute(Tuple &tuple) {
			Relation &relation = _attributeMeta._relation;
			AttributeEntry ret(
				relation.read<std::string>(tuple, 1),
				static_cast<attribute_enum>(relation.read<int>(tuple, 2))
			);
			ret._size = static_cast<page_address>(relation.read<int>(tuple, 3));
			ret._index = static_cast<address>(relation.read<long long>(tuple, 4));
			ret._offset = static_cast<page_address>(relation.read<int>(tuple, 5));
			ret._vCount = static_cast<address>(relation.read<long long>(tuple, 6));
			return std::move(ret);
		}

		inline Tuple toTuple(const AttributeEntry &entry, const std::string &relationName, TupleContainer &container) {
			container.resize(_attributeMeta._relation.maxTupleSize());
			return _attributeMeta._relation.builder().start(container)
				.build(relationName, 0)
				.build(entry._name, 1)
				.build(static_cast<int>(entry._type), 2)
				.build(static_cast<int>(entry._size), 3)
				.build(static_cast<long long>(entry._index), 4)
				.build(static_cast<int>(entry._offset), 5)
				.build(static_cast<long long>(entry._vCount), 6)
				.result();
		}

		inline bool addAttribute(const AttributeEntry &entry, const std::string &relationName) {
			_relations[relationName]._attributes.push_back(entry);
			return true;
		}
	
		inline address toIndex(Tuple &tuple, std::string &relationName, size_t index) {
			Relation &relation = _indexMeta._relation;
			relationName = relation.read<std::string>(tuple, 0);
			index = static_cast<size_t>(relation.read<long long>(tuple, 1));
			return static_cast<address>(relation.read<long long>(tuple, 2));
		}

		inline Tuple toTuple(const std::string &relationName, size_t index, address ptr, TupleContainer &container) {
			container.resize(_indexMeta._relation.maxTupleSize());
			return _attributeMeta._relation.builder().start(container)
				.build(relationName, 0)
				.build(static_cast<long long>(index), 1)
				.build(static_cast<long long>(ptr), 2)
				.result();
		}

		inline bool addIndex(const std::string &relationName, size_t index, address ptr) {
			TupleContainer tmp(0);
			Tuple t = toTuple(relationName, index, ptr, tmp);
			_indexMeta.allocate(t);
			return true;
		}
	};

	struct Controller {
		constexpr static size_t DATA_BLOCK_CAPACITY = SEGMENT_SIZE / 8;
		Keeper _keeper;
		MetadataGuard _metadata;
		std::unordered_map<std::string, RelationGuard *> _relations;
		std::unordered_map<std::string, IndexGuard<int> *> _intIndexes;
		std::unordered_map<std::string, IndexGuard<std::string> *> _stringIndexes;

		Controller(const char *path, bool truncate = false): _keeper(path, truncate), _metadata(_keeper) {
			_keeper.start();
			_metadata.load();
			for (auto & p: _metadata._relations) {
				if (p.first != "RelationMeta" && p.first != "AttributeMeta" && p.first != "IndexMeta") {
					_relations[p.first] = new RelationGuard(_keeper, p.second);
				}
			}
			for (auto &p : _metadata._indexes) {
				auto ptr = p.second;
				auto type = _relations[p.first.first]->_relation._attributes[p.first.second]._type;
				if (type == INT_T) {
					_intIndexes[p.first.first + std::to_string(p.first.second)] = new IndexGuard<int>(&_keeper, p.second);
				} else {
					_stringIndexes[p.first.first + std::to_string(p.first.second)] = new IndexGuard<std::string>(&_keeper, p.second);
				}
			}
			_keeper._translator._params[0].first = 1;
		}

		~Controller() {
			for (auto &p : _intIndexes) {
				delete p.second;
			}
			_intIndexes.clear();
			for (auto &p : _stringIndexes) {
				delete p.second;
			}
			_stringIndexes.clear();
			for (auto &p : _relations) {
				delete p.second;
			}
			_relations.clear();
			_metadata.dump();
			_keeper.close();
		}

		void createRelation(Relation &relation) {
			auto size = _keeper._translator._params[0].first;
			relation._capacity = DATA_BLOCK_CAPACITY;
			relation._ptr = relation._begin = relation._end 
				= db::Translator::segmentBegin(db::DATA_SEG) * db::SEGMENT_SIZE + size * DATA_BLOCK_CAPACITY;
			if (relation._begin == db::Translator::segmentEnd(db::DATA_SEG)) {
				throw std::runtime_error("[Controller::createRelation]");
			}
			_metadata.addRelation(relation);
			_relations[relation._name] = new RelationGuard(_keeper, _metadata._relations[relation._name]);
		}

		void createIndex(std::string relationName, size_t index) {
			address ptr = 0;
			auto type = _relations[relationName]->_relation._attributes[index]._type;
			if (type == INT_T) {
				auto &tmp = _intIndexes[relationName + std::to_string(index)] = new IndexGuard<int>(&_keeper);
				ptr = tmp->iroot;
			} else {
				auto &tmp = _stringIndexes[relationName + std::to_string(index)] = new IndexGuard<std::string>(&_keeper);
				ptr = tmp->iroot;
			}
			_metadata.addIndex(relationName, index, ptr);
		}

		RelationGuard &relationGuard(const std::string &name) {
			if (name == "RelationMeta") {
				return _metadata._relationMeta;
			} else if (name == "AttributeMeta") {
				return _metadata._attributeMeta;
			} else if (name == "RelationMeta") {
				return _metadata._indexMeta;
			} else {
				return *_relations[name];
			}
		}

		inline address fetchIndex(const std::string & rel, size_t index, const std::string &key) {
			return _stringIndexes[rel + std::to_string(index)]->fetch(key);
		}

		inline address fetchIndex(const std::string & rel, size_t index, int key) {
			return _intIndexes[rel + std::to_string(index)]->fetch(key);
		}

		inline void allocateIndex(const std::string & rel, size_t index, const std::string &key, address value) {
			_stringIndexes[rel + std::to_string(index)]->allocate(key, value);
		}

		inline void allocateIndex(const std::string & rel, size_t index, int key, address value) {
			_intIndexes[rel + std::to_string(index)]->allocate(key, value);
		}

		inline void freeIndex(const std::string & rel, size_t index, const std::string &key) {
			_stringIndexes[rel + std::to_string(index)]->free(key);
		}

		inline void freeIndex(const std::string & rel, size_t index, int key) {
			_intIndexes[rel + std::to_string(index)]->free(key);
		}

		inline void reallocateIndex(const std::string & rel, size_t index, const std::string &key, address value) {
			_stringIndexes[rel + std::to_string(index)]->reallocate(key, value);
		}

		inline void reallocateIndex(const std::string & rel, size_t index, int key, address value) {
			_intIndexes[rel + std::to_string(index)]->reallocate(key, value);
		}

		inline void printIndex(const std::string & rel, size_t index) {
			auto p = rel + std::to_string(index);
			{
				auto iter = _stringIndexes.find(p);
				if (iter != _stringIndexes.end()) {
					iter->second->print(15);
					return;
				}
			}
			{
				auto iter = _intIndexes.find(p);
				if (iter != _intIndexes.end()) {
					iter->second->print(5);
					return;
				}
			}
			throw std::runtime_error("[Controller::printIndex]");
		}

		inline Tuple fetchTuple(const std::string & rel, address addr, TupleContainer &container) {
			return relationGuard(rel).fetch(addr, container);
		}

		inline address allocateTuple(const std::string & rel, Tuple &tuple) {
			return relationGuard(rel).allocate(tuple);
		}

		inline void freeTuple(const std::string & rel, address addr) {
			relationGuard(rel).free(addr);
		}

		inline address reallocateTuple(const std::string &rel, address addr, Tuple &tuple) {
			return relationGuard(rel).reallocate(addr, tuple);
		}

		template<typename Function>
		inline void traverseTuple(const std::string &rel, Function fn) {
			relationGuard(rel).traverseTuple(fn);
		}
	};
}