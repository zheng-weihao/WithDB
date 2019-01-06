#pragma once

#include "endian.hpp"
#include "definitions.hpp"
#include "keeper.hpp"
#include "relation.hpp"
#include "utils.hpp"

#include <algorithm>
#include <iterator>
#include <type_traits>
#include <utility>
#include <vector>

namespace db {
	namespace ns::tuple {
		template<typename Iter>
		using check_char_t = std::enable_if_t<std::is_same_v<typename std::iterator_traits<Iter>::value_type, char>>;

		/*template<typename Iter>
		using check_random_access_t = std::enable_if_t<std::is_same_v<typename std::iterator_traits<Iter>::iterator_category, std::random_access_iterator_tag>>;*/
	}

	struct TupleEntry {
		page_address _index;
		page_address _begin;
		page_address _end;
		bool _isComplete;
		bool _isHead;
		bool _isDeleted; // deleted
		inline TupleEntry(page_address index = 0, page_address begin = PAGE_SIZE, page_address end = PAGE_SIZE, bool isComplete = true, bool isHead = true, bool isDeleted = false) :
			_index(index), _begin(begin), _end(end), _isComplete(isComplete), _isHead(isHead), _isDeleted(isDeleted) {
		}

		inline page_address size() {
			return _end - _begin;
		}
	};

	struct TuplePage : VirtualPage {
		constexpr static page_address FLAGS_POS = 0;
		constexpr static page_address USED_SIZE_POS = 2;
		constexpr static page_address FRONT_POS = 4;
		constexpr static page_address BACK_POS = 6;
		constexpr static page_address HEADER_SIZE = 8;

		constexpr static page_address TUPLE_ENTRY_SIZE = 4;
		constexpr static page_address TUPLE_ENTRY_INDEX_POS = 0;
		constexpr static page_address TUPLE_ENTRY_COMPLETE_BIT = 15;
		constexpr static page_address TUPLE_ENTRY_HEAD_BIT = 14;
		constexpr static page_address TUPLE_ENTRY_DELETED_BIT = 13;
		constexpr static page_address TUPLE_ENTRY_PTR_POS = 2;

		// TODO: flags store other information
		page_address _flags;
		page_address _usedSize;
		page_address _front;
		page_address _back;

		std::vector<TupleEntry> _entries;
	public:
		inline TuplePage(container_type &container, size_t first, size_t last, Keeper &keeper, address addr, address length = PAGE_SIZE) : VirtualPage(container, first, last, keeper, addr, length), _flags(0) {
		}

		virtual bool load() {
			reactivate(true);
			_flags = read<page_address>(FLAGS_POS);
			if (!_flags) {
				return false;
			}
			_usedSize = read<page_address>(USED_SIZE_POS);
			_front = read<page_address>(FRONT_POS);
			_back = read<page_address>(BACK_POS);
			_entries.resize((_front - HEADER_SIZE) / TUPLE_ENTRY_SIZE);
			auto last = static_cast<page_address>(PAGE_SIZE);
			size_t i = 0;
			for (auto &entry : _entries) {
				auto offset = HEADER_SIZE + i * TUPLE_ENTRY_SIZE;
				auto index = read<page_address>(offset + TUPLE_ENTRY_INDEX_POS);
				auto ptr = read<page_address>(offset + TUPLE_ENTRY_PTR_POS);
				entry._index = resetFlags(index, TUPLE_ENTRY_DELETED_BIT);
				entry._begin = ptr;
				entry._end = last;
				entry._isComplete = getFlag(index, TUPLE_ENTRY_COMPLETE_BIT);
				entry._isHead = getFlag(index, TUPLE_ENTRY_HEAD_BIT);
				entry._isDeleted = getFlag(index, TUPLE_ENTRY_DELETED_BIT);
				last = ptr;
				++i;
			}
			orderByIndex();
			unpin();
			return true;
		}

		virtual bool dump() {
			if (!_flags) {
				return false;
			}
			orderByPosition();
			reactivate(true);
			if (_flags) {
				write(_flags, FLAGS_POS);
				write(_usedSize, USED_SIZE_POS);
				write(_front, FRONT_POS);
				write(_back, BACK_POS);
				page_address i = HEADER_SIZE;
				for (auto &entry : _entries) {
					auto index = entry._index;
					index = setFlag(index, entry._isComplete, TUPLE_ENTRY_COMPLETE_BIT);
					index = setFlag(index, entry._isHead, TUPLE_ENTRY_HEAD_BIT);
					index = setFlag(index, entry._isDeleted, TUPLE_ENTRY_DELETED_BIT);
					auto ptr = entry._begin;
					write(index, i + TUPLE_ENTRY_INDEX_POS);
					write(ptr, i + TUPLE_ENTRY_PTR_POS);
					i += TUPLE_ENTRY_SIZE;
				}
			}
			unpin();
			orderByIndex();
			return true;
		}

		inline void init() {
			_flags = 1;
			_front = HEADER_SIZE;
			_usedSize = HEADER_SIZE;
			_back = PAGE_SIZE;
		}

		inline void orderByIndex() {
			std::sort(_entries.begin(), _entries.end(), [](const TupleEntry &a, const TupleEntry &b) {
				return a._index < b._index;
			});
		}

		inline void orderByPosition() {
			std::sort(_entries.begin(), _entries.end(), [](const TupleEntry &a, const TupleEntry &b) {
				return a._begin > b._begin;
			});
		}

		inline page_address space(bool fast = true) {
			return fast ? _back - _front : PAGE_SIZE - _usedSize;
		}

		// return pos
		inline size_t fetch(page_address index) {
			TupleEntry tmp(index);
			auto iter = std::lower_bound(_entries.begin(), _entries.end(), tmp, [](const TupleEntry &a, const TupleEntry &b) {
				return a._index < b._index;
			});
			if (iter == _entries.end() || iter->_index != index || iter->_isDeleted) {
				return _entries.size();
			}
			return iter - _entries.begin();
		}

		// return pos
		inline size_t allocate(page_address size, bool fast = true) {
			if (size + TUPLE_ENTRY_SIZE > space(fast)) {
				return _entries.size();
			}
			if (size + TUPLE_ENTRY_SIZE > space()) { // don't need to sweep when fast mode space is enough
				sweep();
			}
			auto iter = _entries.begin();
			page_address tmp = 0;
			for (; iter != _entries.end() && iter->_index == tmp; ++iter, ++tmp) {
			}
			_usedSize += size + TUPLE_ENTRY_SIZE;
			_front += TUPLE_ENTRY_SIZE;
			_back -= size;
			auto ret = iter - _entries.begin();
			_entries.emplace(iter, tmp, _back, _back + size);
			return ret;
		}

		inline size_t free(page_address index) {
			auto pos = fetch(index);
			if (pos != _entries.size()) {
				_entries[pos]._isDeleted = true;
				_usedSize -= static_cast<page_address>(_entries[pos].size());
			}
			return pos;
		}

		inline void sweep() {
			orderByPosition();
			reactivate();
			while (!pin()) {
			}
			_front = HEADER_SIZE;
			_back = PAGE_SIZE;
			for (auto &entry : _entries) {
				if (!entry._isDeleted) {
					_front += TUPLE_ENTRY_SIZE;
					auto tmp = entry._end;
					entry._end = _back;
					while (tmp-- > entry._begin) {
						write(read<char>(--_back), tmp);
					}
					entry._begin = _back;
				}
			}
			unpin();
			_usedSize = PAGE_SIZE - _back + _front;
			_entries.erase(std::remove_if(_entries.begin(), _entries.end(), [](const TupleEntry &e) {
				return e._isDeleted;
			}));
			orderByIndex();
		}

		inline page_address tupleSize(size_t pos) {
			return _entries[pos]._end - _entries[pos]._begin;
		}

		// copy without checking outer layer should check
		template<typename Iter,
			ns::tuple::check_char_t<Iter> * = nullptr
		> inline void copy_to(Iter out, size_t pos) {
			reactivate(true);
			for (auto i = _entries[pos]._begin; i != _entries[pos]._end; ++i) {
				*out++ = read<char>(i);
			}
			unpin();
		}

		template<typename Iter,
			ns::tuple::check_char_t<Iter> * = nullptr
		> inline void copy_from(Iter in, size_t pos) {
			reactivate(true);
			for (auto i = _entries[pos]._begin; i != _entries[pos]._end; ++i) {
				write(*in++, i);
			}
			unpin();
		}
	};

	struct RelationGuard {
		constexpr static address SIZE_HYPER = 4;

		constexpr static address pageAddress(address addr) {
			return addr & ~(PAGE_SIZE - 1);
		}
		constexpr static page_address pageIndex(address addr) {
			return static_cast<page_address>(addr & (PAGE_SIZE - 1));
		}

		Keeper &_keeper;
		Relation &_relation;

		inline RelationGuard(Keeper &keeper, Relation &relation) : _keeper(keeper), _relation(relation) {
		}

		template<typename Function>
		inline void traversePage(Function fn) {
			for (auto ptr = _relation._begin; ptr != _relation._end; ptr += PAGE_SIZE) {
				auto p = _keeper.hold<TuplePage>(ptr, true, false);
				if (!p->load()) {
					continue;
				}
				fn(*p, ptr);
			}
		}

		template<typename Function>
		inline void traverseTuple(Function fn) {
			traversePage([fn](TuplePage &page, address addr) {
				size_t i = 0;
				for (auto &entry : page._entries) {
					if (entry._isDeleted) {
						continue;
					}
					TupleContainer tmp(entry.size());
					Tuple tuple(tmp, 0, tmp.size());
					page.copy_to(tuple.begin(), i);
					fn(tuple, addr + entry._index);
					++i;
				}
			});
		}

		inline Tuple fetch(address addr, TupleContainer &container) {
			auto p = _keeper.hold<TuplePage>(pageAddress(addr));
			auto result = p->fetch(pageIndex(addr));
			if (result == p->_entries.size()) {
				throw std::runtime_error("[RelationGuard::fetch]");
			}
			container.resize(p->tupleSize(result));
			Tuple tuple(container, 0, container.size());
			p->copy_to(tuple.begin(), result);
			return tuple;
		}

		inline address allocate(Tuple &tuple) {
			if (tuple.size() > PAGE_SIZE) { // TODO: support cross page tuple
				throw std::runtime_error("[RelationGuard::allocate]");
			}
			auto ptr = _relation._ptr;
			auto begin = _relation._begin, end = _relation._end, size = end - begin;
			if (!size) {
				_relation._end = end = SIZE_HYPER * PAGE_SIZE;
				size = end - begin;
			}
			for (address i = 0; i != size; ++i) {
				auto p = _keeper.hold<TuplePage>(ptr, true, false);
				if (!p->load()) {
					p->init();
					++_relation._bCount;
				}
				auto result = p->allocate(static_cast<page_address>(tuple.size()));
				if (result != p->_entries.size()) {
					_relation._ptr = ptr;
					++_relation._tCount;
					p->copy_from(tuple.begin(), result);
					p->dump();
					return ptr + p->_entries[result]._index;
				}
				ptr += PAGE_SIZE;
				if (ptr == end) {
					if (size - size / SIZE_HYPER < _relation._bCount && size != _relation._capacity) {
						size = std::max((size / 4 + size) & ~(PAGE_SIZE - 1), _relation._capacity);
						end = begin + size;
						_relation._end = size;
					} else {
						ptr = begin;
					}
				}
			}
			throw std::runtime_error("[RelationGuard::allocate]");
		}
		
		inline void free(address addr) {
			auto p = _keeper.hold<TuplePage>(pageAddress(addr));
			auto result = p->free(pageIndex(addr));
			if (result == p->_entries.size()) {
				throw std::runtime_error("[RelationGuard::free]");
			}
			--_relation._tCount;
			if (p->space(false) == PAGE_SIZE) {
				p.reset();
				_keeper.loosen(addr);
				--_relation._bCount;
			}
		}

		inline address reallocate(address addr, Tuple &tuple) {
			free(addr);
			return allocate(tuple);
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
	};

	// TODO: tuple put in many pages
	// changable length concept class with random access iterator
	/*template<typename Iter,
		ns::tuple::enable_if_char_iterator_t<Iter> * = nullptr,
		ns::tuple::enable_if_random_iterator_t<Iter> * = nullptr
	>
		struct basic_tuple {
		template<typename Iter>
		basic_tuple(Iter first, Iter last) : first, last) {
		}

		template<typename Value>
		inline Value get(iterator first, iterator last) {
			return get_primitive<Value>(first, last);
		}

		template<>
		inline std::string get<std::string>(iterator first, iterator last) {
			return get_string(first, last);
		}

		template<typename Value>
		Value get(page_address first) {
			return get<Value>(begin() + first, end());
		}


		template<typename Value>
		inline void set(Value value, iterator first, iterator last) {
			set_primitive(value, first, last);
		}

		template<>
		inline void set<std::string>(std::string value, iterator first, iterator last) {
			set_string(value, first, last);
		}

		template<typename Value>
		void set(Value value, page_address first) {
			set(value, begin() + first, end());
		}

		tuple() {
		}
	};*/
}