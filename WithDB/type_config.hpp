#ifndef __TYPE_CONFIG_HPP__
#define __TYPE_CONFIG_HPP__

#include <chrono>
#include <cstdint>
#include <random>
#include <string>

namespace db {
	using timestamp = std::int64_t;
	
	using drive_address = std::uint64_t;
	using address = std::uint64_t;
	using cache_address = std::uint32_t; // useless
	using page_address = std::uint16_t;

	// shrink for storage
	using free_page_offset = std::int16_t; // TODO: rename module
	using shrink_segment_pos_address = std::uint32_t;
	using shrink_mapping_key_address = std::uint32_t;

	enum segment_enum {
		METADATA_SEG,
		DATA_SEG,
		BLOB_SEG,
		INDEX_SEG,
		DUMMY_SEG,
	};
	using segment_enum_type = std::uint8_t;

	using element_type = char;
	using char_type = std::string;
	using varchar_type = std::string;
	using date_type = std::string;
	using int_type = std::int32_t;
	using long_type = std::int64_t;
	using float_type = float;
	using double_type = double;


	enum attribute_type_enum {
		CHAR_T,
		VARCHAR_T,
		INT_T,
		LONG_T,
		FLOAT_T,
		DOUBLE_T,
		DATE_T,
		BLOB_T,
		DUMMY_T,
	};
	using attribute_type_enum_type = std::uint8_t;

	constexpr std::size_t SEGMENT_BIT_LENGTH = 32;
	constexpr address SEGMENT_SIZE = static_cast<address>(1) << SEGMENT_BIT_LENGTH;
	constexpr std::size_t PAGE_BIT_LENGTH = 12;
	constexpr address PAGE_SIZE = static_cast<address>(1) << PAGE_BIT_LENGTH;
	constexpr drive_address FIXED_IO_ENTRY_PAGE = 0;
	constexpr drive_address FIXED_TRANSLATOR_ENTRY_PAGE = FIXED_IO_ENTRY_PAGE + PAGE_SIZE;
	constexpr drive_address FIXED_SIZE = FIXED_TRANSLATOR_ENTRY_PAGE + PAGE_SIZE;
	constexpr drive_address INIT_SIZE = PAGE_SIZE * 0x100;
	constexpr drive_address INIT_SYSTEM_SIZE = PAGE_SIZE * 0x60;
	constexpr drive_address INIT_USER_SIZE = INIT_SIZE - FIXED_SIZE - INIT_SYSTEM_SIZE;
	constexpr drive_address EXPAND_SIZE = PAGE_SIZE * 0x20;
	constexpr drive_address SHRINK_SIZE = PAGE_SIZE * 0x10;

	constexpr std::size_t TRANSLATOR_CACHE_SIZE = 0x800;
	constexpr std::size_t KEEPER_CACHE_TOTAL_SIZE = 0x400;
	constexpr std::size_t KEEPER_CACHE_LEVEL = 3;
	constexpr std::size_t KEEPER_CACHE_LEVEL_SIZES[KEEPER_CACHE_LEVEL] = { 0x20, 0x80, 0x300 };

	inline timestamp current_timestamp() {
		return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	}

	inline address default_segment_address(segment_enum e) {
		switch (e) {
		case db::METADATA_SEG:
			return 0 * SEGMENT_SIZE;
		case db::DATA_SEG:
			return 6 * SEGMENT_SIZE;
		case db::BLOB_SEG:
			return 8 * SEGMENT_SIZE;
		case db::INDEX_SEG:
			return 1 * SEGMENT_SIZE;
		default:
			throw std::runtime_error("[default_segment_address] should not access here");
		}
	}

	inline std::size_t segment_cache_level(segment_enum e) {
		switch (e) {
		case db::METADATA_SEG:
			return 0;
		case db::DATA_SEG:
			return 2;
		case db::BLOB_SEG:
			return 2;
		case db::INDEX_SEG:
			return 1;
		default:
			throw std::runtime_error("[default_segment_address] should not access here");
		}
	}
}

#endif // __TYPE_CONFIG_HPP__
