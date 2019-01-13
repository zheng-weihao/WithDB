#pragma once

#include "keeper.hpp"
#include "relation.hpp"
#include "relation_guard.hpp"
// #include "index_guard.hpp"
#include "query.hpp"

#include <functional>
#include <iostream>
#include <string>
#include <sstream>
#include <map>

namespace db {
	struct MetaGuard {
		Keeper &_keeper;
		Schema _schema;
		RelationGuard _relationMetaGuard;
		RelationGuard _attributeMetaGuard;
		RelationGuard _indexMetaGuard;

	public:
		inline MetaGuard(Keeper &keeper) : _keeper(keeper), _schema()
			, _relationMetaGuard(_keeper, _schema.relation(Schema::RELATION_META_POS))
			, _attributeMetaGuard(_keeper, _schema.relation(Schema::ATTRIBUTE_META_POS))
			, _indexMetaGuard(_keeper, _schema.relation(Schema::INDEX_META_POS)) {
		}

		inline void load() {
			auto p = _keeper.hold<VirtualPage>(NULL_ADDRESS, false, false, false);
			size_t pos = 0;
			for (size_t i = Schema::RELATION_META_POS; i <= Schema::INDEX_META_POS; ++i) {
				auto & r = _schema.relation(i);
				if ((r._end = p->read<address>(pos)) == NULL_ADDRESS) {
					r._end = r._begin;
				}
				r._tCount = p->read<size_t>(pos += sizeof(address));
				r._pCount = p->read<size_t>(pos += sizeof(size_t));
				pos += sizeof(size_t);
			}
			p.unpin();
			_relationMetaGuard.traverseTuple([this](Tuple &tuple, address addr) {
				_schema.loadRelation(tuple);
			});
			_attributeMetaGuard.traverseTuple([this](Tuple &tuple, address addr) {
				_schema.loadAttribute(tuple);
			});
			for (auto r : _schema._relations) {
				r->format(true);
			}
			_indexMetaGuard.traverseTuple([this](Tuple &tuple, address addr) {
				_schema.loadIndex(tuple);
			});
		}

		inline void dump() {
			{
				std::vector<address> relationAddresses(_schema._relations.size(), NULL_ADDRESS);
				_relationMetaGuard.traverseTuple([this, &relationAddresses](Tuple &tuple, address addr) {
					auto &a = relationAddresses[tuple.get<int_t>(1)];
					if (a) {
						throw std::runtime_error("[MetaGuard::dump]");
					} else {
						a = addr;
					}
				});
				for (auto &p : _schema._relationNames) {
					if (p.second <= Schema::INDEX_META_POS) { // skip system relatons
						continue;
					}
					auto tuple = std::move(_schema.dumpRelation(p.second));
					auto addr = relationAddresses[p.second];
					if (addr) {
						_relationMetaGuard.reallocate(addr, tuple);
					} else {
						throw std::runtime_error("[MetaGuard::dump]");
					}
				}
			}
			{
				// TODO: only reallocate if updated
				std::unordered_map<size_t, address> attributeAddresses;
				_attributeMetaGuard.traverseTuple([this, &attributeAddresses](Tuple &tuple, address addr) {
					if (!attributeAddresses.try_emplace(Schema::getIndexKey(tuple.get<int_t>(0), tuple.get<int_t>(2)), addr).second) {
						throw std::runtime_error("[MetaGuard::dump]");
					}
				});
				for (auto &rp : _schema._relationNames) {
					if (rp.second <= Schema::INDEX_META_POS) { // skip system relatons
						continue;
					}
					for (auto &ap : _schema.relation(rp.second)._attributeNames) {
						auto iter = attributeAddresses.find(Schema::getIndexKey(rp.second, ap.second));
						if (iter == attributeAddresses.end()) {
							throw std::runtime_error("[MetaGuard::dump]");
						}
						auto addr = iter->second;
						auto tuple = std::move(_schema.dumpAttribute(rp.second, ap.second));
						_attributeMetaGuard.reallocate(addr, tuple);
					}
				}
			}
			{
				std::unordered_map<size_t, address> indexAddresses;
				_indexMetaGuard.traverseTuple([this, &indexAddresses](Tuple &tuple, address addr) {
					if (!indexAddresses.try_emplace(Schema::getIndexKey(tuple.get<int_t>(0), tuple.get<int_t>(1)), addr).second) {
						throw std::runtime_error("[MetaGuard::dump]");
					}
				});
				for (auto &p : _schema._indexes) {
					auto tuple = std::move(_schema.dumpIndex(p.first));
					auto addr = indexAddresses[p.first];
					_indexMetaGuard.reallocate(addr, tuple);
				}
			}
			auto p = _keeper.hold<VirtualPage>(0, false, true, true);
			size_t pos = 0;
			for (size_t i = Schema::RELATION_META_POS; i <= Schema::INDEX_META_POS; ++i) {
				auto & r = _schema.relation(i);
				p->write(r._end, pos);
				p->write(r._tCount, pos += sizeof(address));
				p->write(r._pCount, pos += sizeof(size_t));
				pos += sizeof(size_t);
			}
		}

		inline bool createRelation(Relation &&relation, size_t pos) {
			auto name = relation.name();
			if (!relation.isFormatted()) {
				return false;
			}
			if (!_schema.createRelation(std::move(relation), pos)) {
				return false;
			}
			{
				auto tuple = _schema.dumpRelation(name);
				_relationMetaGuard.allocate(tuple);
			}
			auto rpos = _schema.relationPos(name);
			auto &r = _schema.relation(rpos);
			for (auto p : r._attributeNames) {
				auto tuple = _schema.dumpAttribute(rpos, p.second);
				_attributeMetaGuard.allocate(tuple);
			}
			return true;
		}

		template<typename Key>
		inline bool dropRelaton(const Key &key) {
			// TODO: delete table in the storage
			auto pos = _schema.relationPos(key);
			if (pos == _schema._relations.size() || !_schema.dropRelation(pos)) {
				return false;
			}
			{
				std::vector<address> relationAddresses;
				_relationMetaGuard.traverseTuple([this, &relationAddresses, pos](Tuple &tuple, address addr) {
					if (tuple.get<int_t>(1) == pos) {
						relationAddresses.push_back(addr);
					}
				});
				for (auto addr : relationAddresses) {
					_relationMetaGuard.free(addr);
				}
			}
			{
				std::vector<address> attributeAddresses;
				_attributeMetaGuard.traverseTuple([this, &attributeAddresses, pos](Tuple &tuple, address addr) {
					if (tuple.get<int_t>(0) == pos) {
						attributeAddresses.push_back(addr);
					}
				});
				for (auto addr : attributeAddresses) {
					_attributeMetaGuard.free(addr);
				}
			}
			{
				std::vector<address> indexAddresses;
				_indexMetaGuard.traverseTuple([this, &indexAddresses, pos](Tuple &tuple, address addr) {
					if (tuple.get<int_t>(0) == pos) {
						indexAddresses.push_back(addr);
					}
				});
				for (auto addr : indexAddresses) {
					_indexMetaGuard.free(addr);
				}
			}
			return true;
		}

		template<typename RelationKey, typename AttributeKey>
		inline bool createIndex(const RelationKey &rkey, const AttributeKey & akey, address root) {
			if (!root || !_schema.setIndex(rkey, akey, root)) {
				return false;
			}
			auto tuple = std::move(rkey, akey);
			_indexMetaGuard.allocate(tuple);
			return true;
		}
		
		template<typename RelationKey, typename AttributeKey>
		inline bool dropIndex(const RelationKey &rkey, const AttributeKey & akey) {
			throw std::runtime_error("MetaGuard::dropIndex::NotImplemented");
			auto rpos = relationPos(rkey), apos = attributePos(akey);
			if (_schema.setIndex(rpos, apos))
			{
				std::vector<address> indexAddresses;
				_indexMetaGuard.traverseTuple([this, &indexAddresses, rpos, apos](Tuple &tuple, address addr) {
					if (tuple.get<int_t>(0) == rpos && tuple.get<int_t>(1) == apos) {
						indexAddresses.push_back(addr);
					}
				});
				for (auto addr : indexAddresses) {
					_indexMetaGuard.free(addr);
				}
			}
		}
	};

	struct Controller {
		constexpr static size_t DATA_CAPACITY = SEGMENT_SIZE / 8;
		constexpr static size_t TEMP_CAPACITY = SEGMENT_SIZE / 8;

		Keeper _keeper;
		MetaGuard _metaGuard;

		std::vector<RelationGuard *> _dataGuards;
		std::vector<RelationGuard *> _tempGuards;

		inline Controller(const string &path, bool truncate = false): _keeper(path, truncate), _metaGuard(_keeper) {
			load();
		}

		inline ~Controller() {
			dump();
		}

		inline Schema &schema() { return _metaGuard._schema; }

		template<typename Key>
		inline Relation &relation(const Key &key) { return schema().relation(key); }

		template<typename Key>
		inline RelationGuard &relationGuard(const Key &key) {
			auto &s = schema();
			auto pos = s.relationPos(key);
			if (pos == s._relations.size()) {
				throw std::runtime_error("[Controller::relationGuard]");
			}
			return *_dataGuards[pos];
		}

		inline void load() {
			_metaGuard.load();
			_dataGuards.resize(schema()._relations.size());
			for (auto &p : schema()._relationNames) {
				RelationGuard *ptr = nullptr;
				switch (p.second) {
				case Schema::RELATION_META_POS:
					ptr = &_metaGuard._relationMetaGuard; break;
				case Schema::ATTRIBUTE_META_POS:
					ptr = &_metaGuard._attributeMetaGuard; break;
				case Schema::INDEX_META_POS:
					ptr = &_metaGuard._indexMetaGuard; break;
				default:
					ptr = new RelationGuard(_keeper, relation(p.second)); break;
				}
				_dataGuards[p.second] = ptr;
			}
		}

		inline void dump() {
			for (auto &p : schema()._relationNames) {
				if (p.second <= Schema::INDEX_META_POS) {
					continue;
				}
				delete _dataGuards[p.second];
			}
			_dataGuards.clear();
			for (size_t i = 0; i != _tempGuards.size(); ++i) {
				if (_tempGuards[i]) {
					dropTemp(i);
				}
			}
			for (auto &ptr : _tempGuards) {
				if (ptr) {
					ptr->clear();
					delete ptr;
				}
			}
			_tempGuards.clear();
			_metaGuard.dump();
		}

		inline size_t createRelation(Relation &&relation) {
			size_t i = Schema::INDEX_META_POS + 1;
			for (; i < _dataGuards.size(); ++i) {
				if (_dataGuards[i] == nullptr) {
					break;
				}
			}
			if (i == _dataGuards.size()) {
				if (i == Schema::INDEX_META_POS + 1 + DATA_SEG_CAPACITY * (SEGMENT_SIZE / DATA_CAPACITY)) {
					return 0;
				} else {
					_dataGuards.resize(i + 1);
				}
			}
			relation._capacity = DATA_CAPACITY;
			relation._ptr = relation._begin = relation._end = DATA_SEG_BEGIN +  (i - Schema::INDEX_META_POS - 1) * DATA_CAPACITY;
			auto name = relation.name();
			if (_metaGuard.createRelation(std::move(relation), i)) {
				_dataGuards[i] = new RelationGuard(_keeper, schema().relation(name));
				return i;
			} else {
				return 0;
			}
		}

		template<typename Key>
		inline bool dropRelation(const Key &key) {
			auto pos = schema().relationPos(key);
			if (pos <= Schema::INDEX_META_POS || pos == schema()._relations.size()) {
				return false;
			}
			relationGuard(pos).clear();
			_metaGuard.dropRelaton(pos);
		}

		inline size_t createTemp(const Relation &relation) {
			size_t i;
			for (i = 0; i < _tempGuards.size(); ++i) {
				if (_tempGuards[i] == nullptr) {
					break;
				}
			}
			if (i == _tempGuards.size()) {
				if (i == TEMP_SEG_CAPACITY * (SEGMENT_SIZE / TEMP_CAPACITY)) {
					return false;
				} else {
					_tempGuards.resize(i + 1);
				}
			}
			auto ptr = new Relation(relation);
			{
				auto &relation = *ptr;
				relation._capacity = TEMP_CAPACITY;
				relation._ptr = relation._begin = relation._end = TEMP_SEG_BEGIN + i * TEMP_CAPACITY;
			}
			_tempGuards[i] = new RelationGuard(_keeper, *ptr);
			return static_cast<size_t>(- 1 - i);
		}

		inline bool dropTemp(size_t i) {
			i = static_cast<size_t>(- 1 - i);
			if (!_tempGuards[i]) {
				return false;
			}
			_tempGuards[i]->clear();
			delete &(_tempGuards[i]->_relation);
			delete _tempGuards[i];
			_tempGuards[i] = nullptr;
			return true;
		}

		inline RelationGuard &getGuard(size_t pos) {
			if (getFlag(pos, 63)) {
				return *(_tempGuards[static_cast<size_t>(- 1 - pos)]);
			} else {
				return relationGuard(pos);
			}
		}

		inline size_t query(UnaryQueryStep &step) {
			auto &single = getGuard(step._single);
			auto ret = createTemp(step._result);
			auto &dest = getGuard(ret);
			single.traverseTuple([&](Tuple &tuple, address addr) {
				if (step._selection(tuple)) {
					TupleBuilder builder = dest._relation.builder(true);
					size_t i = 0;
					for (auto j : step._projection) {
						if (!getFlag(j, 63)) {
							builder.build(j, tuple.get<string>(i));
						}
						++i;
					}
					auto tuple = std::move(builder.complete());
					dest.allocate(tuple);
				}
			});
			return ret;
		}

		inline size_t query(BinaryQueryStep &step) {
			auto &left = getGuard(step._left);
			auto &right = getGuard(step._right);
			auto ret = createTemp(step._result);
			auto &dest = getGuard(ret);
			left.traverseTuple([&](Tuple &tuple1, address addr1) {
				right.traverseTuple([&](Tuple &tuple2, address addr2) {
					if (!step._join(tuple1, tuple2)) {
						return;
					}
					TupleBuilder builder = dest._relation.builder(true);
					int i = 0;
					for (i = 0; i < left._relation._attributes.size(); ++i) {
						if (!getFlag(step._projection[i], 63)) {
							builder.build(step._projection[i], tuple1.get<string>(i));
						}
					}
					for (int j = 0; j < right._relation._attributes.size(); ++j, ++i) {
						if (!getFlag(step._projection[i], 63)) {
							builder.build(step._projection[i], tuple2.get<string>(j));
						}
					}
					auto tmp = std::move(builder.complete());
					dest.allocate(tmp);
				});
			});
			return ret;
		}

		template<typename Key>
		inline void printAll(const Key &key) { // DEBUG
			auto &rg = relationGuard(key);
			auto &r = rg._relation;
			std::cout << "|";
			for (auto &a : r._attributes) {
				std::cout << a._name << "|";
			}
			std::cout << std::endl;
			rg.traverseTuple([](Tuple &tuple, address addr) {
				auto size = tuple._relation._attributes.size();
				std::cout << "|";
				for (auto i = 0; i < size; ++i) {
					std::cout << tuple.get<string>(i) << "|";
				}
				std::cout << std::endl;
			});
		}

		inline void printResult(size_t pos) {
			auto &g = getGuard(pos);
			auto &r = g._relation;
			std::cout << "|";
			for (auto &a : r._attributes) {
				std::cout << a._name << "|";
			}
			std::cout << std::endl;
			g.traverseTuple([](Tuple &tuple, address addr) {
				auto size = tuple._relation._attributes.size();
				std::cout << "|";
				for (auto i = 0; i < size; ++i) {
					std::cout << tuple.get<string>(i) << "|";
				}
				std::cout << std::endl;
			});
		}

		template<typename Key>
		inline address createTuple(const Key & key, Tuple &tuple) {
			return relationGuard(key).allocate(tuple);
		}

		template<typename Key>
		inline address updateTuple(const Key &key, address addr, Tuple &tuple) {
			return relationGuard(key).rellocate(addr, tuple);
		}

		template<typename Key>
		inline Tuple retrieveTuple(const Key & key, address addr) {
			return relationGuard(key).fetch(addr);
		}

		template<typename Key>
		inline void deleteTuple(const Key &key, address addr) {
			relationGuard(key).free(addr);
		}

		/*void unionOp();
		void diffOp();
		void intersectOp();
		void selectOp();
		void projectOp();
		void productOp();
		void renameOp();
		void joinOp();
		void groupOp();
		void distinctOp();

		void insertOp();
		void updateOp();
		void deleteOp();*/

		/*void createIndex(std::string relationName, size_t index) {
			address ptr = 0;
			auto type = _relations[relationName]->_relation._attributes[index]._type;
			if (type == INT_T) {
				auto &tmp = _intIndexes[relationName + toString(index)] = new IndexGuard<int>(&_keeper);
				ptr = tmp->iroot;
			} else {
				auto &tmp = _stringIndexes[relationName + toString(index)] = new IndexGuard<std::string>(&_keeper);
				ptr = tmp->iroot;
			}
			_metaGuard.addIndex(relationName, index, ptr);
		}*/

		

		

		//inline void freeTuple(const std::string & rel, address addr) {
		//	relationGuard(rel).free(addr);
		//}

		//inline address reallocateTuple(const std::string &rel, address addr, Tuple &tuple) {
		//	return relationGuard(rel).reallocate(addr, tuple);
		//}

		//template<typename Function>
		//inline void traverseTuple(const std::string &rel, Function fn) {
		//	relationGuard(rel).traverseTuple(fn);
		//}

		//inline address fetchIndex(const std::string & rel, size_t index, const std::string &key) {
		//	return _stringIndexes[rel + toString(index)]->fetch(key);
		//}

		//inline address fetchIndex(const std::string & rel, size_t index, int key) {
		//	return _intIndexes[rel + toString(index)]->fetch(key);
		//}

		//inline void allocateIndex(const std::string & rel, size_t index, const std::string &key, address value) {
		//	_stringIndexes[rel + toString(index)]->allocate(key, value);
		//}

		//inline void allocateIndex(const std::string & rel, size_t index, int key, address value) {
		//	_intIndexes[rel + toString(index)]->allocate(key, value);
		//}

		//inline void freeIndex(const std::string & rel, size_t index, const std::string &key) {
		//	_stringIndexes[rel + toString(index)]->free(key);
		//}

		//inline void freeIndex(const std::string & rel, size_t index, int key) {
		//	_intIndexes[rel + toString(index)]->free(key);
		//}

		//inline void reallocateIndex(const std::string & rel, size_t index, const std::string &key, address value) {
		//	_stringIndexes[rel + toString(index)]->reallocate(key, value);
		//}

		//inline void reallocateIndex(const std::string & rel, size_t index, int key, address value) {
		//	_intIndexes[rel + toString(index)]->reallocate(key, value);
		//}

		//inline void printIndex(const std::string & rel, size_t index) {
		//	auto p = rel + toString(index);
		//	{
		//		auto iter = _stringIndexes.find(p);
		//		if (iter != _stringIndexes.end()) {
		//			iter->second->print(15);
		//			return;
		//		}
		//	}
		//	{
		//		auto iter = _intIndexes.find(p);
		//		if (iter != _intIndexes.end()) {
		//			iter->second->print(5);
		//			return;
		//		}
		//	}
		//	throw std::runtime_error("[Controller::printIndex]");
		//}
	};
}