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
	struct segment_entry {
		address pos;
		drive_address mapping_ptr;
		segment_enum seg;
		
		segment_entry(address pos, drive_address mapping_ptr, segment_enum seg) : pos(pos), mapping_ptr(mapping_ptr), seg(seg) {
		}

		segment_entry(const segment_entry &other) : segment_entry(other.pos, other.mapping_ptr, other.seg) {
		}
	};

	struct translator_page : page {
		constexpr static page_address DATABASE_NAME_BEGIN = 0;
		constexpr static page_address DATABASE_NAME_END = 64;
		constexpr static page_address DATABASE_NAME_SIZE = DATABASE_NAME_END - DATABASE_NAME_BEGIN;

		// deprecate
		// constexpr static page_address SYSTEM_SEGMENT_TABLE_SIZE_POS = 252;
		// constexpr static page_address USER_SEGMENT_TABLE_SIZE_POS = 254;

		constexpr static page_address SEGMENT_TABLE_SIZE_POS = 254;

		constexpr static page_address SEGMENT_ENTRY_POS_POS = 0;
		constexpr static page_address SEGMENT_ENTRY_SEG_POS = 4;
		constexpr static page_address SEGMENT_ENTRY_PTR_POS = 8;
		constexpr static page_address SEGMENT_ENTRY_SIZE = 16;

		constexpr static page_address SEGMENT_TABLE_BEGIN = 256;
		constexpr static page_address SEGMENT_TABLE_END = 4096;
		constexpr static page_address SEGMENT_TABLE_SIZE = (SEGMENT_TABLE_END - SEGMENT_TABLE_BEGIN) / SEGMENT_ENTRY_SIZE;

		std::vector<segment_entry> segment_table;

	public:
		translator_page(iterator first, iterator last) : basic_page(first, last) {
		}
		translator_page() {
		}

		virtual void load() {
			auto segment_table_size = read<page_address>(SEGMENT_TABLE_SIZE_POS);
			segment_table.clear();
			for (page_address i = 0; i != segment_table_size; ++i) {
				auto offset = SEGMENT_ENTRY_SIZE * i + SEGMENT_TABLE_BEGIN;
				auto shrink_pos = read<shrink_segment_pos_address>(offset + SEGMENT_ENTRY_POS_POS);
				auto seg = read<segment_enum_type>(offset + SEGMENT_ENTRY_SEG_POS);
				auto ptr = read<drive_address>(offset + SEGMENT_ENTRY_PTR_POS);
				segment_table.emplace_back(
					static_cast<address>(shrink_pos) << SEGMENT_BIT_LENGTH,
					ptr, static_cast<segment_enum>(seg)
				);
			}
		}

		virtual void dump() {
			if (segment_table.size() > SEGMENT_ENTRY_SIZE) {
				throw std::out_of_range("[translator_entry_page::dump] segment_table are out of range");
			}
			write(static_cast<page_address>(segment_table.size()), SEGMENT_TABLE_SIZE_POS);
			page_address i = SEGMENT_TABLE_BEGIN;
			for (auto &entry : segment_table) {
				write(static_cast<shrink_segment_pos_address>(entry.pos >> SEGMENT_BIT_LENGTH), i + SEGMENT_ENTRY_POS_POS);
				write(static_cast<segment_enum_type>(entry.seg), i + SEGMENT_ENTRY_SEG_POS);
				write(entry.mapping_ptr, i + SEGMENT_ENTRY_PTR_POS);
				i += SEGMENT_ENTRY_SIZE;
			}
		}

		std::string get_database_name() {
			return read<std::string>(DATABASE_NAME_BEGIN, DATABASE_NAME_END);
		}

		void set_database_name(const std::string &name) {
			write(name, DATABASE_NAME_BEGIN, DATABASE_NAME_END);
		}

	};

	struct mapping_entry {
		address key;
		drive_address value;

		mapping_entry(address key, drive_address value) : key(key), value(value) {
		}

		mapping_entry(const mapping_entry &other) : mapping_entry(other.key, other.value) {
		}
	};

	struct mapping_page: page {
		constexpr static page_address NEXT_PTR_POS = 0;

		constexpr static page_address MAPPING_TABLE_SIZE_POS = 14;
		constexpr static page_address MAPPING_ENTRY_SIZE = 10; // 3 byte for key, 7 byte for value
		constexpr static page_address MAPPING_ENTRY_KEY_POS = 0;
		constexpr static page_address MAPPING_ENTRY_KEY_SIZE = 3;
		constexpr static page_address MAPPING_ENTRY_VALUE_POS = 2;
		constexpr static page_address MAPPING_ENTRY_VALUE_SIZE = 7;
		constexpr static page_address MAPPING_TABLE_BEGIN = 16;
		constexpr static page_address MAPPING_TABLE_END = 4096;
		constexpr static page_address MAPPING_TABLE_SIZE = (MAPPING_TABLE_END - MAPPING_TABLE_BEGIN) / MAPPING_ENTRY_SIZE;

		drive_address next_ptr;

		std::vector<mapping_entry> mapping_table;

		inline mapping_page(iterator first, iterator last) : basic_page(first, last) {
		}

		virtual void load() {
			next_ptr = read<drive_address>(NEXT_PTR_POS);

			constexpr page_address value_offset = MAPPING_ENTRY_SIZE - static_cast<page_address>(sizeof(drive_address));

			auto mapping_table_size = read<page_address>(MAPPING_TABLE_SIZE_POS);
			mapping_table.clear();
			for (page_address i = 0; i != mapping_table_size; ++i) {
				auto offset = MAPPING_ENTRY_SIZE * i + MAPPING_TABLE_BEGIN;
				auto shrink_key = read<shrink_mapping_key_address>(offset + MAPPING_ENTRY_KEY_POS) >> (8 * (sizeof(shrink_mapping_key_address) - MAPPING_ENTRY_KEY_SIZE));
				auto value = read<drive_address>(offset + MAPPING_ENTRY_VALUE_POS) & ((1ll << MAPPING_ENTRY_VALUE_SIZE * 8) - 1);
				// TODO: hack way for reading non-stdint data, need better definitions support
				mapping_table.emplace_back(
					static_cast<address>(shrink_key) << PAGE_BIT_LENGTH,
					value << PAGE_BIT_LENGTH
				);
			}
		}

		virtual void dump() {
			write(next_ptr, NEXT_PTR_POS);

			if (mapping_table.size() > MAPPING_TABLE_SIZE) {
				throw std::out_of_range("[mapping_page::dump] mapping_table are out of range");
			}
			write(static_cast<page_address>(mapping_table.size()), MAPPING_TABLE_SIZE_POS);
			page_address i = MAPPING_TABLE_BEGIN;
			// TODO: so ugly
			for (auto &entry : mapping_table) {
				auto shrink_key = static_cast<shrink_mapping_key_address>(entry.key >> PAGE_BIT_LENGTH);
				auto value = entry.value >> PAGE_BIT_LENGTH;
				write(shrink_key << 8, i + MAPPING_ENTRY_KEY_POS);
				write(value | (static_cast<drive_address>(shrink_key) << (8 * MAPPING_ENTRY_VALUE_SIZE)), i + MAPPING_ENTRY_VALUE_POS);
				i += MAPPING_ENTRY_SIZE;
			}
		}
	};
	
	// TODO: load all mapping pages without cache in manager, need to improve
	struct translator: cache_handler<address, drive_address> {
		drive &io;
		std::vector<std::vector<char>> memories;
		translator_page entry;
		std::vector<std::vector<mapping_page>> mappings;
		cache<address, drive_address> lookaside;

	public:
		translator(drive &io) : io(io), lookaside(TRANSLATOR_CACHE_SIZE, *this) {
			memories.emplace_back(PAGE_SIZE);
			entry.set_pair_ptr(memories[0].begin(), memories[0].end());
			io.get(entry, FIXED_TRANSLATOR_ENTRY_PAGE);
			if (entry.segment_table.empty()) {
				init();
			} else {
				load();
			}
		}

		void close() {
			save();
		}

		// TODO: free mapping page and free segment
		void init() {
			const segment_enum default_segment[] = { METADATA_SEG, DATA_SEG, BLOB_SEG, INDEX_SEG,};
			for (auto i = 0; i != 4; ++i) {
				add_segment(default_segment[i], default_segment_address(default_segment[i]));
			}
			save();
		}

		void load() {
			for (auto iter = entry.segment_table.begin(); iter != entry.segment_table.end(); ++iter) {
				mappings.emplace_back();
				auto &seg = mappings.back();
				auto addr = iter->mapping_ptr;
				while (addr) {
					memories.emplace_back(PAGE_SIZE);
					seg.emplace_back(memories.back().begin(), memories.back().end());
					io.get(seg.back(), addr);
					addr = seg.back().next_ptr;
				}
				
			}
		}

		void save() {
			io.put(entry, FIXED_TRANSLATOR_ENTRY_PAGE);
			for (std::size_t i = 0; i != entry.segment_table.size(); ++i) {
				auto addr = entry.segment_table[i].mapping_ptr;
				auto &seg = mappings[i];
				for (auto iter = seg.begin(); iter != seg.end(); ++iter) {
					io.put(*iter, addr);
					addr = iter->next_ptr;
				}
			}
		}

		void add_segment(segment_enum seg, address addr) {
			entry.segment_table.emplace_back(addr, 0, seg);
			mappings.emplace_back();
		}

		void add_segment(segment_enum seg) {
			address addr = entry.segment_table.empty() ? 0 : entry.segment_table.back().pos + SEGMENT_SIZE;
			add_segment(seg, addr);
		}

		inline std::size_t find_segment_index(address addr) {
			auto iter = std::find_if(entry.segment_table.begin(), entry.segment_table.end(), [addr](const segment_entry &e) {
				return addr >= e.pos && addr < e.pos + SEGMENT_SIZE;
			});
			if (iter == entry.segment_table.end()) {
				throw std::out_of_range("access address out of any segment");
			}
			return static_cast<std::size_t>(iter - entry.segment_table.begin());
		}

		inline segment_enum find_seg(address addr) {
			return entry.segment_table[find_segment_index(addr)].seg;
		}

		void add_mapping(std::size_t index) {
			// TODO: new mapping page allocate strategy
			auto ptr = io.allocate(0, true);
			auto &mapping = mappings[index];
			if (!mapping.empty()) {
				mapping.back().next_ptr = ptr;
			} else {
				entry.segment_table[index].mapping_ptr = ptr;
			}
			memories.emplace_back(PAGE_SIZE);
			mapping.emplace_back(memories.back().begin(), memories.back().end());
			mapping.back().next_ptr = 0;
			save();
		}

		void link(address addr, drive_address ptr) {
			auto index = find_segment_index(addr);
			mapping_entry item(addr - entry.segment_table[index].pos, ptr);
			auto &seg = mappings[index];
			for (auto iter = seg.begin(); iter != seg.end(); ++iter) {
				if (iter->mapping_table.size() < mapping_page::MAPPING_TABLE_SIZE) {
					auto &table = iter->mapping_table;
					auto pos = std::lower_bound(table.begin(), table.end(), item, [](const mapping_entry &a, const mapping_entry &b) {
						return a.key < b.key;
					});
					table.insert(pos, item);
					return;
				}
			}
			add_mapping(index);
			seg.back().mapping_table.push_back(item);
			// TODO: write back strategy
		}

		void unlink(address addr) {
			auto index = find_segment_index(addr);
			auto &seg = mappings[index];
			mapping_entry item(addr - entry.segment_table[index].pos, 0);
			for (auto iter = seg.begin(); iter != seg.end(); ++iter) {
				auto &table = iter->mapping_table;
				auto pos = std::lower_bound(table.begin(), table.end(), item, [](const mapping_entry &a, const mapping_entry &b) {
					return a.key < b.key;
				});
				if (pos != table.end() && pos->key == item.key) {
					table.erase(pos);
					return;
				}
			}
			throw std::runtime_error("cannot find address");
		}

		drive_address operator()(address addr) {
			return lookaside.get(addr);
		}

		bool is_pinned(address addr) {
			lookaside.is_pinned(addr);
		}

		void pin(address addr) {
			(*this)(addr);
			lookaside.pin(addr);
		}

		void unpin(address addr) {
			if (is_pinned(addr)) {
				lookaside.unpin(addr);
			}
		}

		virtual bool cache_insert(address addr, drive_address &value) {
			auto index = find_segment_index(addr);
			auto &seg = mappings[index];
			mapping_entry item(addr - entry.segment_table[index].pos, 0);
			for (auto iter = seg.begin(); iter != seg.end(); ++iter) {
				auto &table = iter->mapping_table;
				auto pos = std::lower_bound(table.begin(), table.end(), item, [](const mapping_entry &a, const mapping_entry &b) {
					return a.key < b.key;
				});
				if (pos != table.end() && pos->key == item.key) {
					value = pos->value;
					return true;
				}
			}
			return false;
		}
		
		virtual bool cache_erase(address addr, drive_address &value) {
			return true;
		}
	};
}