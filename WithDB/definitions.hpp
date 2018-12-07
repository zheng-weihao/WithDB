#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <chrono>
#include <random>
#include <string>

namespace db {
	// get the complement of the check(enable_if) SFINAE type
	template<typename Check, typename Type = void>
	struct check_not { using type = Type; };
	template<typename Check>
	struct check_not<Check, std::void_t<typename Check::type>> {};
	template<typename Check>
	using check_not_t = typename check_not<Check>::type;
	// TODO: figure out why the code here doesn't work in template partial specialization
	/*template<typename Check>
	struct complement<Check, typename Check::type> {};*/

	// transfrom check SFINAE type to value
	template<typename Check, typename Type = void>
	struct check_value : std::false_type {};
	template<typename Check>
	struct check_value<Check, std::void_t<typename Check::type>> : std::true_type {};
	template<typename Check>
	using check_v = check_value<Check>::value;

	// TODO: define switch structure for checking
	// switch_type1_t<...Param>, switch_type2_t<...Param>, ..., switch_default_t<...Param>
	// need a method to construct switch structure that have different template id to replace void_t below
	// to simplify checking template parameters
	/*template<typename Type, typename Iter>
	struct switch_network_t : std::void_t<check_network_order_t<Type>, check_iterator_t<Type>> {};
	template<typename Type, typename Iter>
	struct switch_string_t : std::void_t<check_not_network_order_t<Type>, check_string_t<Type>, check_iterator_t<Iter>> {};
	template<typename Type, typename Iter>
	struct switch_default_t : std::void_t<check_not_network_order_t<Type>, check_not_string_t<Type>, check_iterator_t<Iter>> {};*/

	// uint<Size> using for endian, might have other potential usage
	template<std::size_t Size>
	struct uint {};
	template<> struct uint<1> { using type = std::uint8_t; };
	template<> struct uint<2> { using type = std::uint16_t; };
	template<> struct uint<4> { using type = std::uint32_t; };
	template<> struct uint<8> { using type = std::uint64_t; };
	template<std::size_t Size>
	using uint_t = typename uint<Size>::type;

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

	// TODO: exception system
}
