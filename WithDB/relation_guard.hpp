#pragma once

#include "endian.hpp"
#include "definitions.hpp"
#include "keeper.hpp"
#include "relation.hpp"
#include "utils.hpp"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <iterator>
#include <type_traits>
#include <utility>
#include <vector>

namespace db {
	namespace ns::tuple {
		template<typename Iter>
		using check_element_t = std::enable_if_t<std::is_same_v<typename std::iterator_traits<Iter>::value_type, element_t>>;
	}

	struct TupleEntry {
		enum flag_enum {
			DELETED_FLAG,
			TUPLE_FLAG,
			HEAD_FLAG,
			BODY_FLAG,
		};

		page_address _index;
		page_address _begin;
		page_address _end;
		element_t _flag;
		// bool _access; // true when index is accessible, false when delete and incomplete part of a tuple which is not head, so they can't access by this index
		// bool _piece; // so item is delete when _access = false and _piece = fa;se
		
		inline TupleEntry(page_address index = NULL_ADDRESS, page_address begin = NULL_ADDRESS
			, page_address end = NULL_ADDRESS, flag_enum flag = DELETED_FLAG)
			: _index(index), _begin(begin), _end(end), _flag(flag) {
		}

		inline page_address size() { return _end - _begin; }
		inline bool isDeleted() { return _flag == DELETED_FLAG; }
		inline bool isTuple() { return _flag == TUPLE_FLAG; }
		inline bool isHead() { return _flag == HEAD_FLAG; }
		inline bool isPart() { return _flag == BODY_FLAG; }
		inline bool isAccess() { return isTuple() || isHead(); }
	};

	struct TuplePage : VirtualPage {
		using entry_data = std::uint32_t;

		constexpr static size_t USED_POS = 0;
		constexpr static size_t FRONT_POS = USED_POS + sizeof(page_address);
		constexpr static size_t BACK_POS = FRONT_POS + sizeof(page_address);
		constexpr static size_t HEADER_SIZE = BACK_POS + sizeof(page_address);

		constexpr static size_t TUPLE_ENTRY_SIZE = sizeof(entry_data); // store as uint32
		constexpr static size_t TUPLE_ENTRY_INDEX_BEGIN = 0;
		constexpr static size_t TUPLE_ENTRY_INDEX_END = TUPLE_ENTRY_INDEX_BEGIN + PAGE_BIT_LENGTH; // index and offset will not reach 0x1000
		constexpr static size_t TUPLE_ENTRY_OFFSET_BEGIN = TUPLE_ENTRY_INDEX_END;
		constexpr static size_t TUPLE_ENTRY_OFFSET_END = TUPLE_ENTRY_OFFSET_BEGIN + PAGE_BIT_LENGTH;
		constexpr static size_t TUPLE_ENTRY_FLAG_BEGIN = TUPLE_ENTRY_OFFSET_END;
		constexpr static size_t TUPLE_ENTRY_FLAG_END = TUPLE_ENTRY_SIZE * 8;

		page_address _used;
		page_address _front;
		page_address _back;

		std::vector<TupleEntry> _entries;
	public:
		inline TuplePage(Keeper &keeper, size_t flags) : VirtualPage(keeper, flags) {
		}

		virtual bool load() {
			_used = read<page_address>(USED_POS);
			if (!_used) {
				return false;
			}
			
			_front = read<page_address>(FRONT_POS);
			_back = read<page_address>(BACK_POS);
			_entries.resize((_front - HEADER_SIZE) / TUPLE_ENTRY_SIZE);

			auto rptr = static_cast<page_address>(PAGE_SIZE);
			size_t i = 0;
			for (auto &entry : _entries) {
				auto data = read<entry_data>(HEADER_SIZE + i * TUPLE_ENTRY_SIZE);
				entry._index = static_cast<page_address>(getFlag(data, TUPLE_ENTRY_INDEX_BEGIN, TUPLE_ENTRY_INDEX_END));
				auto offset = static_cast<page_address>(getFlag(data, TUPLE_ENTRY_OFFSET_BEGIN, TUPLE_ENTRY_OFFSET_END));
				entry._begin = offset;
				entry._end = rptr;
				rptr = entry._begin;
				entry._flag = static_cast<TupleEntry::flag_enum>(getFlag(data, TUPLE_ENTRY_FLAG_BEGIN, TUPLE_ENTRY_FLAG_END));
				++i;
			}
			orderByIndex();
			return true;
		}

		virtual bool dump() {
			orderByPosition();
			write(_used, USED_POS);
			write(_front, FRONT_POS);
			write(_back, BACK_POS);
			size_t i = HEADER_SIZE;
			for (auto &entry : _entries) {
				entry_data data = entry._index;
				data = setFlag(data, entry._begin, TUPLE_ENTRY_OFFSET_BEGIN, TUPLE_ENTRY_OFFSET_END);
				data = setFlag(data, entry._flag, TUPLE_ENTRY_FLAG_BEGIN, TUPLE_ENTRY_FLAG_END);
				write(data, i);
				i += TUPLE_ENTRY_SIZE;
			}
			orderByIndex();
			return true;
		}

		inline void init() {
			_used = HEADER_SIZE;
			_front = HEADER_SIZE;
			_back = PAGE_SIZE;
		}

		inline void orderByIndex() {
			std::sort(_entries.begin(), _entries.end(), [](const TupleEntry &a, const TupleEntry &b) {
				return a._index < b._index;
			});
		}

		inline void orderByPosition() {
			// big offset first
			std::sort(_entries.begin(), _entries.end(), [](const TupleEntry &a, const TupleEntry &b) {
				return a._begin > b._begin;
			});
		}

		inline page_address space(bool sweep = false) {
			return sweep ? _back - _front : PAGE_SIZE - _used;
		}

		// return pos
		inline size_t fetch(page_address index) {
			TupleEntry tmp(index);
			auto iter = std::lower_bound(_entries.begin(), _entries.end(), tmp, [](const TupleEntry &a, const TupleEntry &b) {
				return a._index < b._index;
			});
			if (iter != _entries.end() && iter->_index == index && !iter->isDeleted()) {
				return iter - _entries.begin();
			}
			return _entries.size();
		}

		// return pos
		inline size_t allocate(page_address size, bool sweep = false) {
			if (size + TUPLE_ENTRY_SIZE > space(sweep)) {
				return _entries.size();
			}
			if (size + TUPLE_ENTRY_SIZE > space(false)) { // don't need to sweep when fast mode space is enough
				this->sweep();
			}
			auto iter = _entries.begin();
			page_address tmp = 0;
			for (; iter != _entries.end() && iter->_index == tmp; ++iter, ++tmp) {
			}
			_used += size + TUPLE_ENTRY_SIZE;
			_front += TUPLE_ENTRY_SIZE;
			_back -= size;
			auto ret = iter - _entries.begin();
			_entries.emplace(iter, tmp, _back, _back + size, TupleEntry::flag_enum::TUPLE_FLAG);
			return ret;
		}

		inline size_t free(page_address index) {
			auto pos = fetch(index);
			if (pos != _entries.size()) {
				_entries[pos]._flag = TupleEntry::flag_enum::DELETED_FLAG;
				_used -= static_cast<page_address>(_entries[pos].size());
			}
			return pos;
		}

		inline void sweep() {
			orderByPosition();
			_front = HEADER_SIZE;
			_back = PAGE_SIZE;
			auto ptr = data();
			for (auto &entry : _entries) {
				if (!entry.isDeleted()) {
					_front += TUPLE_ENTRY_SIZE;
					auto tmp = entry._end;
					entry._end = _back;
					while (tmp > entry._begin) {
						*(ptr + --_back) = *(ptr + --tmp);
					}
					entry._begin = _back;
				}
			}
			_used = PAGE_SIZE - _back + _front;
			_entries.erase(std::remove_if(_entries.begin(), _entries.end(), [](const TupleEntry &e) {
				return e._flag;
			}));
			orderByIndex();
		}

		inline page_address size(size_t pos) {
			return _entries[pos]._end - _entries[pos]._begin;
		}

		// copy without checking outer layer should check
		template<typename Iter,
			ns::tuple::check_element_t<Iter> * = nullptr
		> inline void copy_to(Iter out, size_t pos) {
			auto b = data(), e = data() + _entries[pos]._end;
			for (b += _entries[pos]._begin; b != e; *out++ = *b++) {
			}
		}

		template<typename Iter,
			ns::tuple::check_element_t<Iter> * = nullptr
		> inline void copy_from(Iter in, size_t pos) {
			auto b = data(), e = data() + _entries[pos]._end;
			for (b += _entries[pos]._begin; b != e; *b++ = *in++) {
			}
		}
	};

	inline void encode(Tuple &tuple) {
		for (auto &attribute : tuple._relation._attributes) {
			auto ptr = tuple.data() + attribute._offset;
			switch (attribute._type) {
			case db::CHAR_T:
			case db::DATE_T:
				break;
			case db::VARCHAR_T:
			case db::FLOAT_T:
			case db::INT_T:
			{
				auto data = reinterpret_cast<std::uint32_t *>(ptr);
				*data = encode(*data);
				break;
			}
			case db::LONG_T:
			case db::DOUBLE_T:
			case db::LOB_T:
			case db::BLOB_T:
			case db::CLOB_T:
			{
				auto data = reinterpret_cast<std::uint64_t *>(ptr);
				*data = encode(*data);
				break;
			}
			default:
				break;
			}
		}
	}

	inline void decode(Tuple &tuple) {
		for (auto &attribute : tuple._relation._attributes) {
			auto ptr = tuple.data() + attribute._offset;
			switch (attribute._type) {
			case db::CHAR_T:
			case db::DATE_T:
				break;
			case db::VARCHAR_T:
			case db::FLOAT_T:
			case db::INT_T:
			{
				auto data = reinterpret_cast<std::uint32_t *>(ptr);
				*data = decode<std::uint32_t>(*data);
				break;
			}
			case db::LONG_T:
			case db::DOUBLE_T:
			case db::LOB_T:
			case db::BLOB_T:
			case db::CLOB_T:
			{
				auto data = reinterpret_cast<std::uint64_t *>(ptr);
				*data = decode<std::uint64_t>(*data);
				break;
			}
			default:
				break;
			}
		}
	}

	// TODO: tuple store in mutli-page
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

		inline bool collect(address addr, Tuple &tuple) {
			auto p = _keeper.hold<TuplePage>(pageAddress(addr), false, false, false);
			if (!p->load()) {
				return false;
			}
			auto result = p->fetch(pageIndex(addr));
			if (result == p->_entries.size()) {
				return false;
			}
			auto &entry = p->_entries[result];
			if (!entry.isAccess()) {
				return false;
			}
			tuple.resize(entry.size());
			p->copy_to(tuple.begin(), result);
			p.unpin();
			decode(tuple);
			return true;
		}

		inline Tuple fetch(address addr) {
			Tuple tuple(_relation);
			if (collect(addr, tuple)) {
				return tuple;
			} else {
				throw std::runtime_error("[RelationGuard::fetch]");
			}
		}

		inline address allocate(Tuple &tuple) {
			if (tuple.size() > PAGE_SIZE - TuplePage::HEADER_SIZE - TuplePage::TUPLE_ENTRY_SIZE) { // TODO: support cross page tuple
				throw std::runtime_error("[RelationGuard::allocate]");
			}
			encode(tuple);
			address ret = NULL_ADDRESS;
			auto &ptr = _relation._ptr, &begin = _relation._begin, &end = _relation._end;
			auto size = end - begin;
			if (!size) {
				end = SIZE_HYPER * PAGE_SIZE + begin;
				size = end - begin;
			}
			bool sweepFlag = size - size / SIZE_HYPER < _relation._pCount * PAGE_SIZE;
			for (address i = 0; i != size; ++i) {
				auto p = _keeper.hold<TuplePage>(ptr, false, false, false);
				if (!p->load()) {
					p->init(); // will also be modified by copy_from (allocate must succeed), so don't have to dump now
					++_relation._pCount;
					sweepFlag = size - size / SIZE_HYPER < _relation._pCount * PAGE_SIZE;
				}
				auto result = p->allocate(static_cast<page_address>(tuple.size()), sweepFlag);
				if (result != p->_entries.size()) {
					++_relation._tCount;
					
					p->copy_from(tuple.begin(), result);
					p->dump();
					p->setDirty(true);
					ret = ptr + p->_entries[result]._index;
					break;
				}
				ptr += PAGE_SIZE;
				if (ptr == end) {
					if (sweepFlag && size != _relation._capacity) {
						size = std::max(pageAddress(size + size / SIZE_HYPER), _relation._capacity);
						end = begin + size;
						sweepFlag = size - size / SIZE_HYPER < _relation._pCount * PAGE_SIZE;
					} else {
						ptr = begin;
					}
				}
			}
			decode(tuple);
			if (ret == NULL_ADDRESS) {
				throw std::runtime_error("[RelationGuard::allocate]");
			} else {
				return ret;
			}
		}
		
		inline void free(address addr) {
			auto p = _keeper.hold<TuplePage>(pageAddress(addr), true, false, false);
			auto result = p->free(pageIndex(addr));
			if (result == p->_entries.size()) {
				throw std::runtime_error("[RelationGuard::free]");
			}
			--_relation._tCount;
			if (p->space(true) == PAGE_SIZE && p._tmp == true) { // page empty, can't handle the situation that page pin by other user after this pinning
				p.unpin();										 // but it works because RelationGuard don't have this pin situation for now
				_keeper.loosen(addr);
				--_relation._pCount;
			} else {
				p->dump();
				p->setDirty(true);
			}
		}

		inline address reallocate(address addr, Tuple &tuple) {
			auto p = _keeper.hold<TuplePage>(pageAddress(addr), true, false, false);
			auto pos = p->fetch(pageIndex(addr));
			if (pos == p->_entries.size()) {
				throw std::runtime_error("[RelationGuard::reallocate]");
			}
			if (p->size(pos) == tuple.size()) {
				encode(tuple);
				p->copy_from(tuple.begin(), pos);
				p->dump();
				p->setDirty(true);
				decode(tuple);
				return addr;
			}
			p.unpin();
			free(addr);
			return allocate(tuple);
		}

		inline void traversePage(const std::function<void(TuplePage &, address)> &fn) {
			for (auto ptr = _relation._begin; ptr != _relation._end; ptr += PAGE_SIZE) {
				auto p = _keeper.hold<TuplePage>(ptr, false, false, false);
				if (!p->load()) {
					continue;
				}
				fn(*p, ptr);
			}
		}

		inline void traverseTuple(const std::function<void(Tuple &, address)> &fn) {
			Tuple tuple(_relation);
			traversePage([&tuple, &fn](TuplePage &page, address addr) {
				size_t i = 0;
				for (auto &entry : page._entries) {
					if (entry.isDeleted()) {
						continue;
					}
					tuple.resize(entry.size());
					page.copy_to(tuple.begin(), i);
					decode(tuple);
					fn(tuple, addr + entry._index);
					++i;
				}
			});
		}

		inline void clear() {
			for (auto ptr = _relation._begin; ptr != _relation._end; ptr += PAGE_SIZE) {
				auto p = _keeper.hold<TuplePage>(ptr, false, false, false);
				if (!p->load()) {
					continue;
				}
				p.unpin();
				_keeper.loosen(ptr);
			}
			_relation._ptr = _relation._end = _relation._begin;
		}
	};
}