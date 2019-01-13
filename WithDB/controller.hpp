#pragma once

#include "keeper.hpp"
#include "relation.hpp"
#include "relation_guard.hpp"
// #include "index_guard.hpp"

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
					if (p.second <= Schema::INDEX_META_POS) {
						continue;
					}
					auto tuple = std::move(_schema.dumpRelation(p.first, p.second));
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
					if (rp.second <= Schema::INDEX_META_POS) {
						continue;
					}
					for (auto &ap : _schema.relation(rp.second)._attributeNames) {
						auto iter = attributeAddresses.find(Schema::getIndexKey(rp.second, ap.second));
						if (iter == attributeAddresses.end()) {
							throw std::runtime_error("[MetaGuard::dump]");
						}
						auto addr = iter->second;
						auto tuple = std::move(_schema.dumpAttribute(rp.second, ap.first, ap.second));
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

		inline bool createRelation(const string &name, Relation &&relation) {
			if (!relation.isFormatted()) {
				return false;
			}
			if (!_schema.createRelation(name, std::move(relation))) {
				return false;
			}
			{
				auto tuple = _schema.dumpRelation(name);
				_relationMetaGuard.allocate(tuple);
			}
			auto rpos = _schema.relationPos(name);
			auto &r = _schema.relation(rpos);
			for (auto p : r._attributeNames) {
				auto tuple = _schema.dumpAttribute(rpos, p.first, p.second);
				_attributeMetaGuard.allocate(tuple);
			}
			return true;
		}

		inline bool dropRelaton(const string &name) {
			// TODO: delete table in the storage
			throw std::runtime_error("MetaGuard::dropRelaton::NotImplemented");
			if (!_schema.dropRelation(name)) {
				return false;
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
			return _schema.setIndex(rkey, akey);
		}
	};

	struct Controller {
		constexpr static size_t DATA_CAPACITY = SEGMENT_SIZE / 8;
		constexpr static size_t TEMP_CAPACITY = SEGMENT_SIZE / 8;

		Keeper _keeper;
		MetaGuard _metaGuard;

		std::unordered_map<string, RelationGuard *> _relationGuards;
		//std::unordered_map<std::string, IndexGuard<int> *> _intIndexes;
		//std::unordered_map<std::string, IndexGuard<std::string> *> _stringIndexes;

		Controller(const string &path, bool truncate = false): _keeper(path, truncate), _metaGuard(_keeper) {
			_metaGuard.load();
			for (auto & p: _metaGuard._schema._relationNames) {
				if (p.second > Schema::INDEX_META_POS) {
					auto res = _relationGuards.try_emplace(p.first, new RelationGuard(_keeper, _metaGuard._schema.relation(p.second)));
					if (!res.second) {
						throw std::runtime_error("[Controller::constructor]");
					}
				}
			}
			/*for (auto &p : _metaGuard._indexes) {
				auto ptr = p.second;
				auto type = _relations[p.first.first]->_relation._attributes[p.first.second]._type;
				if (type == INT_T) {
					_intIndexes[p.first.first + toString(p.first.second)] = new IndexGuard<int>(&_keeper, p.second);
				} else {
					_stringIndexes[p.first.first + toString(p.first.second)] = new IndexGuard<std::string>(&_keeper, p.second);
				}
			}*/
		}

		~Controller() {
			/*for (auto &p : _intIndexes) {
				delete p.second;
			}
			_intIndexes.clear();
			for (auto &p : _stringIndexes) {
				delete p.second;
			}
			_stringIndexes.clear();*/
			for (auto &p : _relationGuards) {
				delete p.second;
			}
			_relationGuards.clear();
			_metaGuard.dump();
		}

		bool createRelation(const string &name, Relation &&relation) {
			for (auto i = DATA_SEG_BEGIN; i != DATA_SEG_END; i += SEGMENT_SIZE) {
				auto &param = _keeper.param(i);
				if (param < 8) {
					relation._capacity = DATA_CAPACITY;
					relation._ptr = relation._begin = relation._end = i + param * DATA_CAPACITY;
					++param;
					if (_metaGuard.createRelation(name, std::move(relation))) {
						auto res = _relationGuards.try_emplace(name, new RelationGuard(_keeper, _metaGuard._schema.relation(name)));
						if (res.second) {
							return true;
						}
					}
					return false;
				}
			}
			return false;
		}

		RelationGuard &relationGuard(const string &name) {
			if (name == "RelationMeta") {
				return _metaGuard._relationMetaGuard;
			} else if (name == "AttributeMeta") {
				return _metaGuard._attributeMetaGuard;
			} else if (name == "IndexMeta") {
				return _metaGuard._indexMetaGuard;
			} else {
				return *_relationGuards[name];
			}
		}

		void printAll(const string &name) { // DEBUG
			auto &rg = relationGuard(name);
			auto &r = rg._relation;
			std::vector<std::pair<string, size_t>> v(r._attributeNames.begin(), r._attributeNames.end());
			std::sort(v.begin(), v.end(), [](const std::pair<string, size_t> &a, const std::pair<string, size_t> &b) {
				return a.second < b.second;
			});
			std::cout << "|";
			for (auto &p : v) {
				std::cout << p.first << "|";
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

		template<typename Key>
		inline Tuple fetchTuple(const Key & key, address addr) {
			return relationGuard(key).fetch(addr);
		}

		template<typename Key>
		inline address insertTuple(const Key & key, Tuple &tuple) {
			return relationGuard(key).allocate(tuple);
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