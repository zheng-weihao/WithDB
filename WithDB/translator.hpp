#pragma once

#include "cache.hpp"
#include "definitions.hpp"
#include "drive.hpp"
#include "page.hpp"

#include <array>
#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace db {
	constexpr segment_enum getSegmentEnum(address addr) {
		if (addr < METADATA_SEG_END) {
			return METADATA_SEG;
		} else if (addr < BLOB_SEG_END) {
			return BLOB_SEG;
		} else if (addr < DATA_SEG_END) {
			return DATA_SEG;
		} else if (addr < INDEX_SEG_END) {
			return INDEX_SEG;
		} else if (addr < TEMP_SEG_END) {
			return TEMP_SEG;
		} else {
			return DUMMY_SEG;
		}
	}

	// TODO: declare segment usage in entry, only data segment is available for now
	struct SegmentEntry {
		drive_address _ptr;
		size_t _param; // integer parameter for different segment type to parse
		inline SegmentEntry(drive_address ptr = 0, std::uint32_t param = 0) : _ptr(ptr), _param(param) {
		}
	};

	struct TranslatorEntryPage : Page {
		constexpr static page_address DATABASE_NAME_BEGIN = 0;
		constexpr static page_address DATABASE_NAME_END = 256;
		constexpr static page_address DATABASE_NAME_CAPACITY = DATABASE_NAME_END - DATABASE_NAME_BEGIN - 1; // robust

		constexpr static page_address SEGMENT_PTR_POS = 0;
		constexpr static page_address SEGMENT_PARAM_POS = SEGMENT_PTR_POS + sizeof(drive_address);
		constexpr static page_address SEGMENT_SIZE = SEGMENT_PARAM_POS + sizeof(size_t);

		constexpr static page_address SEGMENTS_BEGIN = DATABASE_NAME_END;
		constexpr static page_address SEGMENTS_END = PAGE_SIZE;
		constexpr static page_address SEGMENTS_SIZE = (SEGMENTS_END - SEGMENTS_BEGIN) / SEGMENT_SIZE;

		std::string _name;
		std::array<SegmentEntry, SEGMENTS_SIZE> _segments;

	public:
		inline TranslatorEntryPage(Container &container) : Page(container) {
		}

		virtual bool load() {
			_name = read<string>(DATABASE_NAME_BEGIN, DATABASE_NAME_END);
			size_t i = 0;
			for (auto &entry : _segments) {
				auto offset = SEGMENTS_BEGIN + i * SEGMENT_SIZE;
				_segments[i]._ptr = read<drive_address>(offset + SEGMENT_PTR_POS);
				_segments[i]._param = read<size_t>(offset + SEGMENT_PARAM_POS);
				++i;
			}
			return true;
		}

		virtual bool dump() {
			if (_name.size() > DATABASE_NAME_CAPACITY) {
				// throw std::runtime_error("[TranslatorEntryPage::dump] segment_table are out of range");
				return false;
			}
			write(_name, DATABASE_NAME_BEGIN, DATABASE_NAME_END);

			size_t i = SEGMENTS_BEGIN;
			for (auto &entry : _segments) {
				write(entry._ptr, i + SEGMENT_PTR_POS);
				write(entry._param, i + SEGMENT_PARAM_POS);
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
		constexpr static size_t HEADER_SIZE = NEXT_POS + sizeof(drive_address);

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

		inline MappingPage(Container &container, size_t pos) : Page(container, pos), _next(0) {
		}

		virtual bool load() {
			_next = read<drive_address>(NEXT_POS);
			
			if (size() == HEADER_SIZE) {
				return true;
			}

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
	
	// TODO: current: load all mapping pages without Cache in manager, need to improve, partial load some of the mapping pages
	// TODO: algorithm cannot recover from system fault
	struct Translator: BasicCacheHandler<address, drive_address> {
	private:
		constexpr static size_t cacheHash(address addr) {
			return (static_cast<size_t>(addr) >> PAGE_BIT_LENGTH) * 517619 % 69061; // magic prime number
		}
	public:
		using Cache = db::Cache<address, drive_address, Translator, HashCacheCore<address, cacheHash>>;
		Drive &_drive;
		TranslatorEntryPage _entry;
		std::vector<std::vector<MappingEntry>> _mappings; // TODO: temporary structure change structure to partial load
		Cache _lookaside;
	public:
		inline Translator(Drive &drive, size_t capacity = TRANSLATOR_LOOKASIDE_SIZE) : _drive(drive), _entry(_drive.appendFixed(PAGE_SIZE)), _lookaside(*this, capacity) {
			auto pos = _drive.fixedSize() - PAGE_SIZE;
			_entry.activate(pos, pos + PAGE_SIZE);
			if (_drive.isOpen()) {
				open(capacity);
			}
		}

		inline ~Translator() {
			close();
		}

		inline bool isOpen() {
			return _mappings.size() != 0;
		}

		inline void open(size_t capacity = TRANSLATOR_LOOKASIDE_SIZE) {
			if (isOpen() || !_drive.isOpen()) {
				throw std::runtime_error("[Translator::open]");
			}
			_mappings.resize(MAX_SEG_CAPACITY);
			_lookaside.open(capacity);
			load();
		}

		inline void close() {
			if (!isOpen()) {
				return;
			}
			dump();
			_lookaside.close();
			_mappings.clear();
		}

		inline void load() {
			_drive.get(_entry, FIXED_TRANSLATOR_ENTRY_PAGE);
			MappingPage m(_entry._container, _entry._begin);
			size_t i = 0;
			for (auto &seg : _entry._segments) {
				for (auto ptr = seg._ptr; ptr; ptr = m._next) {
					_drive.get(m, ptr);
					_mappings[i].insert(_mappings[i].end(), m._mappings.begin(), m._mappings.end());
				}
				++i;
			}
		}

		inline void dump() {
			MappingPage m(_entry._container, _entry._begin);
			size_t i = 0;
			for (auto &seg : _entry._segments) {
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

				m.resize(MappingPage::HEADER_SIZE); // partial load traverse
				while (ptr) {
					_drive.get(m, ptr);
					_drive.free(ptr, true);
					ptr = m._next;
				}
				m.resize(PAGE_SIZE);
				++i;
			}
			_drive.put(_entry, FIXED_TRANSLATOR_ENTRY_PAGE);
		}

		inline string &name() { return _entry._name; }

		inline size_t &param(address addr) {
			return _entry._segments[addr / SEGMENT_SIZE]._param;
		}

		template<typename Function>
		inline bool search(address addr, Function &fn) { // TODO: need refine onInsert, link, unlink, relink shared logic
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
			if (_lookaside.isOpen()) {
				_lookaside.discard(addr);
			}
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
			if (_lookaside.isOpen()) {
				_lookaside.discard(addr);
			}
			pos->_value = dest;
			return true; 
			// TODO: write back strategy
		}

		inline drive_address operator()(address addr, bool &flag) {
			if (_lookaside.isOpen()) {
				drive_address tmp = 0;
				flag = _lookaside.collect(addr, tmp);
				return tmp;
			}
			drive_address ret = 0;
			flag = onInsert(addr, ret);
			return ret;
		}

		inline drive_address operator()(address addr) {
			bool flag = false;
			auto ret = operator()(addr, flag);
			if (!flag) {
				throw std::runtime_error("translator::operator()");
			}
			return ret;
		}
	};
}