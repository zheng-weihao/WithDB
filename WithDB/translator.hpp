#pragma once

#include "cache.hpp"
#include "drive.hpp"
#include "page.hpp"
#include "definitions.hpp"

#include <algorithm>
#include <stdexcept>
#include <string>
#include <vector>

namespace db {
	// TODO: declare segment usage in entry, only data segment is available for now
	struct SegmentEntry {
		drive_address _ptr;
		std::int32_t _first, _second; // integer aurguement for different segment type to parse
		inline SegmentEntry(drive_address ptr = 0, int first = 0, int second = 0) : _ptr(ptr), _first(first), _second(second) {
		}
	};

	struct TranslatorEntryPage : Page {
		constexpr static page_address DATABASE_NAME_BEGIN = 0;
		constexpr static page_address DATABASE_NAME_END = 256;
		constexpr static page_address DATABASE_NAME_CAPACITY = DATABASE_NAME_END - DATABASE_NAME_BEGIN - 1; // robust

		// deprecate
		// constexpr static page_address SYSTEM_SEGMENT_TABLE_SIZE_POS = 252;
		// constexpr static page_address USER_SEGMENT_TABLE_SIZE_POS = 254;

		constexpr static page_address SEGMENT_PTR_POS = 0;
		constexpr static page_address SEGMENT_FIRST_POS = SEGMENT_PTR_POS + sizeof(drive_address);
		constexpr static page_address SEGMENT_SECOND_POS = SEGMENT_FIRST_POS + sizeof(std::int32_t);
		constexpr static page_address SEGMENT_SIZE = SEGMENT_SECOND_POS + sizeof(std::int32_t);

		constexpr static page_address SEGMENTS_BEGIN = DATABASE_NAME_END;
		constexpr static page_address SEGMENTS_END = 4096;
		constexpr static page_address SEGMENTS_SIZE = (SEGMENTS_END - SEGMENTS_BEGIN) / SEGMENT_SIZE;

		std::string _name;
		std::vector<SegmentEntry> _segments;

	public:
		inline TranslatorEntryPage(Container &container, size_t first, size_t last) : Page(container, first, last) {
		}

		virtual bool load() {
			_name = read<std::string>(DATABASE_NAME_BEGIN, DATABASE_NAME_END);
			_segments.resize(SEGMENTS_SIZE);
			for (size_t i = 0; i != _segments.size(); ++i) {
				auto offset = SEGMENTS_BEGIN + i * SEGMENT_SIZE;
				_segments[i]._ptr = read<drive_address>(offset + SEGMENT_PTR_POS);
				_segments[i]._first = read<std::int32_t>(offset + SEGMENT_FIRST_POS);
				_segments[i]._second = read<std::int32_t>(offset + SEGMENT_SECOND_POS);
			}
			return true;
		}

		virtual bool dump() {
			if (_name.size() > DATABASE_NAME_CAPACITY || _segments.size() > SEGMENTS_SIZE) {
				// throw std::runtime_error("[TranslatorEntryPage::dump] segment_table are out of range");
				return false;
			}
			write(_name, DATABASE_NAME_BEGIN, DATABASE_NAME_END);

			page_address i = SEGMENTS_BEGIN;
			for (auto &entry : _segments) {
				write(entry._ptr, i + SEGMENT_PTR_POS);
				write(entry._first, i + SEGMENT_FIRST_POS);
				write(entry._second, i + SEGMENT_SECOND_POS);
				i += SEGMENT_SIZE;
			}
			return true;
		}
	};

	struct MappingEntry {
		address _key;
		drive_address _value;

		inline MappingEntry(address key = 0, drive_address value = 0) : _key(key), _value(value) {
		}
	};

	struct MappingPage: Page {
		constexpr static page_address NEXT_POS = 0;

		constexpr static page_address MAPPINGS_SIZE_POS = 14;

		using shrinked_key = std::uint32_t;
		constexpr static page_address MAPPING_KEY_SIZE = 3;
		constexpr static page_address MAPPING_VALUE_SIZE = 7;
		constexpr static page_address MAPPING_SIZE = 10; // 3 byte for key, 7 byte for value
		constexpr static page_address MAPPING_KEY_POS = 0;
		constexpr static page_address MAPPING_VALUE_POS = MAPPING_SIZE - static_cast<page_address>(sizeof(drive_address));
		
		constexpr static page_address MAPPINGS_BEGIN = MAPPINGS_SIZE_POS + sizeof(page_address);
		constexpr static page_address MAPPINGS_END = 4096;
		constexpr static page_address MAPPINGS_CAPACITY = (MAPPINGS_END - MAPPINGS_BEGIN) / MAPPING_SIZE;

		drive_address _next;
		std::vector<MappingEntry> _mappings;

		inline MappingPage(Container &container, size_t first, size_t last) : Page(container, first, last) {
			_next = 0;
		}

		virtual bool load() {
			_next = read<drive_address>(NEXT_POS);
			_mappings.resize(read<page_address>(MAPPINGS_SIZE_POS));
			
			for (page_address i = 0; i != _mappings.size(); ++i) {
				auto offset = MAPPINGS_BEGIN + i * MAPPING_SIZE;
				auto key = read<shrinked_key>(offset + MAPPING_KEY_POS) >> (8 * (sizeof(shrinked_key) - MAPPING_KEY_SIZE));
				_mappings[i]._key = static_cast<address>(key) << PAGE_BIT_LENGTH;
				auto value = read<drive_address>(offset + MAPPING_VALUE_POS) & ((1ll << MAPPING_VALUE_SIZE * 8) - 1);
				_mappings[i]._value = value << PAGE_BIT_LENGTH;
				// TODO: hack way for reading non-stdint data, need better definitions support
			}
			return true;
		}

		virtual bool dump() {
			write(_next, NEXT_POS);
			if (_mappings.size() > MAPPINGS_CAPACITY) {
				// throw std::runtime_error("[MappingPage::dump] mapping_table are out of range");
				return false;
			}
			write(static_cast<page_address>(_mappings.size()), MAPPINGS_SIZE_POS);
			page_address i = MAPPINGS_BEGIN;
			for (auto &entry : _mappings) {
				auto key = static_cast<shrinked_key>(entry._key >> PAGE_BIT_LENGTH);
				write(key << 8, i + MAPPING_KEY_POS);
				auto value = entry._value >> PAGE_BIT_LENGTH;
				write(value | (static_cast<drive_address>(key) << (8 * MAPPING_VALUE_SIZE)), i + MAPPING_VALUE_POS);
				i += MAPPING_SIZE;
			}
			return true;
		}

		inline void clear() {
			_next = 0;
			_mappings.clear();
		}
	};
	
	// TODO: load all mapping pages without Cache in manager, need to improve
	// TODO: algorithm cannot recover from system fault
	struct Translator: BasicCacheHandler<address, drive_address> {
	private:
		inline static size_t cacheHash(address addr) {
			return (static_cast<size_t>(addr) >> PAGE_BIT_LENGTH) * 517619 % 69061; // magic prime number
		}
	public:
		Drive &_drive;
		std::string _name;
		Cache<address, drive_address, Translator, HashCacheCore<address, cacheHash>> _lookaside;
		std::vector<std::pair<std::int32_t, std::int32_t>> _params;
		std::vector<std::vector<MappingEntry>> _mappings; // convenient to change structure
		
	public:
		inline Translator(Drive &drive, size_t capacity = LOOKASIDE_SIZE) : _drive(drive), _lookaside(*this) {
			if (_drive.isOpen() && capacity) {
				open(capacity);
			}
		}

		inline ~Translator() {
			close();
		}

		inline bool isOpen() {
			return _lookaside.isOpen();
		}

		inline void open(size_t capacity = LOOKASIDE_SIZE) {
			if (isOpen() || !_drive.isOpen()) {
				throw std::runtime_error("[Translator::open]");
			}
			_lookaside.open(capacity);
			_params.resize(MAX_SEG_CAPACITY);
			_mappings.resize(MAX_SEG_CAPACITY);
			load();
		}

		inline void close() {
			if (!isOpen()) {
				return;
			}
			dump();
			_mappings.clear();
			_params.clear();
			_lookaside.close();
		}

		void load() {
			Container c;
			TranslatorEntryPage entry(c, 0, PAGE_SIZE);
			_drive.get(entry, FIXED_TRANSLATOR_ENTRY_PAGE);
			_name = entry._name;
			//if (!entry._segments[0]._first && !entry._segments[0]._second) { // special check using metadata segment
			//	return; // save time
			//} // DEBUG
			MappingPage m(c, 0, PAGE_SIZE);
			size_t i = 0;
			for (auto &seg : entry._segments) {
				_params[i].first = seg._first;
				_params[i].second = seg._second;
				for (auto ptr = seg._ptr; ptr; ptr = m._next) {
					_drive.get(m, ptr);
					_mappings[i].insert(_mappings[i].end(), m._mappings.begin(), m._mappings.end());
				}
				++i;
			}
		}

		void dump() {
			Container c;
			TranslatorEntryPage entry(c, 0, PAGE_SIZE);
			MappingPage m(c, 0, PAGE_SIZE);
			_drive.get(entry, FIXED_TRANSLATOR_ENTRY_PAGE);
			entry._name = _name;

			size_t i = 0;
			for (auto &seg : entry._segments) {
				seg._first = _params[i].first;
				seg._second = _params[i].second;
				auto iter = _mappings[i].begin(), e = _mappings[i].end();
				auto ptr = seg._ptr;
				if (iter == e) {
					seg._ptr = 0;
				} else {
					if (ptr) {
						_drive.get(m, ptr);
					} else {
						seg._ptr = ptr = _drive.allocate(0, true);
						m.clear();
					}
					while (iter != e) {
						auto tmp = MappingPage::MAPPINGS_CAPACITY < std::distance(iter, e) ? iter + MappingPage::MAPPINGS_CAPACITY : e;
						m._mappings.resize(std::distance(iter, tmp));
						std::copy(iter, tmp, m._mappings.begin());
						iter = tmp;
						auto next = m._next;
						if (iter == e) {
							m._next = 0;
							_drive.put(m, ptr);
							m.clear();
						} else if (!next) {
							m._next = next = _drive.allocate(ptr, true);
							_drive.put(m, ptr);
							m.clear();
						} else {
							_drive.put(m, ptr);
							_drive.get(m, next);
						}
						ptr = next;
					}
				}
				while (ptr) {
					_drive.get(m, ptr);
					_drive.free(ptr, true);
					ptr = m._next;
				}
				++i;
			}
			_drive.put(entry, FIXED_TRANSLATOR_ENTRY_PAGE);
		}

		// TODO: free mapping page and free segment, pre-allocate segment
		//void init() {
		//	const segment_enum default_segment[] = { METADATA_SEG, DATA_SEG, BLOB_SEG, INDEX_SEG,};
		//	for (auto i = 0; i != 4; ++i) {
		//		add_segment(default_segment[i], default_segment_address(default_segment[i]));
		//	}
		//	save();
		//}

		template<typename Function>
		inline bool search(address addr, Function fn) { // TODO: need refine onInsert, link, unlink, relink shared logic
			constexpr auto cmp = [](const MappingEntry &a, const MappingEntry &b) {
				return a._key < b._key;
			};
			auto segmentIndex = addr / SEGMENT_SIZE, pageIndex = addr % SEGMENT_SIZE;
			if (segmentIndex >= MAX_SEG_CAPACITY) {
				return false;
			}
			auto &v = _mappings[segmentIndex];
			MappingEntry tmp(pageIndex);
			auto pos = std::lower_bound(v.begin(), v.end(), tmp, cmp);
			return fn(pos != v.end() && pos->_key == pageIndex, v, pos);
		}

		inline bool onInsert(address addr, drive_address &value) {
			auto segmentIndex = addr / SEGMENT_SIZE, pageIndex = addr % SEGMENT_SIZE;
			if (segmentIndex >= MAX_SEG_CAPACITY) {
				return false;
			}
			auto &v = _mappings[segmentIndex];
			MappingEntry tmp(pageIndex);
			auto pos = std::lower_bound(v.begin(), v.end(), tmp, [](const MappingEntry &a, const MappingEntry &b) {
				return a._key < b._key;
			});
			if (pos == v.end() || pos->_key != pageIndex) {
				return false;
			}
			value = pos->_value;
			return true;
		}

		// create mapping entry
		inline bool link(address addr, drive_address dest) {
			auto segmentIndex = addr / SEGMENT_SIZE, pageIndex = addr % SEGMENT_SIZE;
			if (segmentIndex >= MAX_SEG_CAPACITY) {
				return false;
			}
			auto &v = _mappings[segmentIndex];
			MappingEntry entry(pageIndex, dest);
			auto pos = std::lower_bound(v.begin(), v.end(), entry, [](const MappingEntry &a, const MappingEntry &b) {
				return a._key < b._key;
			});
			if (pos == v.end() || pos->_key != pageIndex) {
				v.insert(pos, entry);
				return true;
			} else {
				return false;
			}
			// TODO: write back strategy
		}

		// delete mapping entry
		inline bool unlink(address addr) {
			auto segmentIndex = addr / SEGMENT_SIZE, pageIndex = addr % SEGMENT_SIZE;
			if (segmentIndex >= MAX_SEG_CAPACITY) {
				return false;
			}
			auto &v = _mappings[segmentIndex];
			MappingEntry entry(pageIndex, 0);
			auto pos = std::lower_bound(v.begin(), v.end(), entry, [](const MappingEntry &a, const MappingEntry &b) {
				return a._key < b._key;
			});
			if (pos == v.end() || pos->_key != pageIndex) {
				return false;
			}
			_lookaside.discard(addr);
			v.erase(pos);
			return true;
		}

		// change mapping entry in sweeping procedure
		inline bool relink(address addr, drive_address dest) {
			auto segmentIndex = addr / SEGMENT_SIZE, pageIndex = addr % SEGMENT_SIZE;
			if (segmentIndex >= MAX_SEG_CAPACITY) {
				return false;
			}
			auto &v = _mappings[segmentIndex];
			MappingEntry entry(pageIndex, dest);
			auto pos = std::lower_bound(v.begin(), v.end(), entry, [](const MappingEntry &a, const MappingEntry &b) {
				return a._key < b._key;
			});
			if (pos == v.end() || pos->_key != pageIndex) {
				return false;
			}
			_lookaside.discard(addr);
			pos->_value = dest;
			return true; 
			// TODO: write back strategy
		}

		inline drive_address operator()(address addr, bool &flag) {
			// return _lookaside.fetch(addr, flag);
			drive_address ret = 0;
			flag = onInsert(addr, ret);
			return ret;
		}

		inline drive_address operator()(address addr) {
			// return _lookaside.fetch(addr);
			bool flag = false;
			auto ret = operator()(addr, flag);
			if (!flag) {
				throw std::runtime_error("translator::operator()");
			}
			return ret;
		}

		constexpr static size_t segmentBegin(segment_enum e) {
			switch (e) {
			case db::DUMMY_SEG:
				return 0;
			case db::METADATA_SEG:
				return 0;
			case db::BLOB_SEG:
				return segmentEnd(METADATA_SEG);
			case db::DATA_SEG:
				return segmentEnd(BLOB_SEG);
			case db::INDEX_SEG:
				return segmentEnd(DATA_SEG);
			case db::TEMP_SEG:
				return segmentEnd(INDEX_SEG);
			default:
				return 0;
			}
		}

		constexpr static size_t segmentEnd(segment_enum e) {
			switch (e) {
			case db::DUMMY_SEG:
				return 0;
			case db::METADATA_SEG:
				return segmentBegin(e) + METADATA_SEG_CAPACITY;
			case db::BLOB_SEG:
				return segmentBegin(e) + BLOB_SEG_CAPACITY;
			case db::DATA_SEG:
				return segmentBegin(e) + DATA_SEG_CAPACITY;
			case db::INDEX_SEG:
				return segmentBegin(e) + INDEX_SEG_CAPACITY;
			case db::TEMP_SEG:
				return segmentBegin(e) + TEMP_SEG_CAPACITY;
			default:
				return 0;
			}
		}
	};
}