#pragma once

#define NOMINMAX // defined for windows.h to use min max in algorithm stl with windows sdk

#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <string>

// DESGIN:
// name rules:
// namespace: db, ns::inner::namespace::for::file::usage
// constexpr / const variable => CONST_NAME
// stl style template type alias defination in class for template type fetch (keep the same with stl in alias but use ClassName to declare and define)
// boost-style and stl style template metaprogramming for compile time decision => check_something (structure like enable if)/ check_something_v (value)/ check_something_t (enable_if_t)
// basic type alias => basic_alias
// object types and alias and template parameters => MyClassName
// member function => myMemberFunction
// member variables => _myMemberVariable
// local variable => ptr for short

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
	constexpr bool check_v = check_value<Check>::value;

	// TODO: define switch structure for checking
	// switch_type1_t<...Param>, switch_type2_t<...Param>, ..., switch_default_t<...Param>
	// need a method to construct switch structure that have different template id to replace void_t below
	// to simplify checking template parameters
	/*template<typename Value, typename Iter>
	struct switch_network_t : std::void_t<check_network_order_t<Value>, check_iterator_t<Value>> {};
	template<typename Value, typename Iter>
	struct switch_string_t : std::void_t<check_not_network_order_t<Value>, check_string_t<Value>, check_iterator_t<Iter>> {};
	template<typename Value, typename Iter>
	struct switch_default_t : std::void_t<check_not_network_order_t<Value>, check_not_string_t<Value>, check_iterator_t<Iter>> {};*/

	// uint<Size> using for endian, might have other potential usage
	template<std::size_t Size>
	struct uint {};
	template<> struct uint<1> { using type = std::uint8_t; };
	template<> struct uint<2> { using type = std::uint16_t; };
	template<> struct uint<4> { using type = std::uint32_t; };
	template<> struct uint<8> { using type = std::uint64_t; };
	template<std::size_t Size>
	using uint_t = typename uint<Size>::type;

	// container fetch index size type
	using size_t = std::size_t;
	// timestamp type for MRU and other statistic
	using timestamp = std::int64_t;
	
	// address type with usage
	using drive_address = std::uint64_t;
	using address = std::uint64_t;
	using cache_address = std::uint32_t; // useless, TODO: discuss about pointer swizzling
	using page_address = std::uint16_t;

	using element_type = char;
	using char_type = std::string;
	using varchar_type = std::string;
	using date_type = std::string;
	using int_type = std::int32_t;
	using long_type = std::int64_t;
	using float_type = float;
	using double_type = double;

	enum segment_enum {
		DUMMY_SEG,
		METADATA_SEG,
		BLOB_SEG,
		DATA_SEG,
		INDEX_SEG,
		TEMP_SEG,
	};
	using segment_enum_type = std::uint8_t;

	enum attribute_enum {
		DUMMY_T,
		CHAR_T,
		VARCHAR_T,
		INT_T,
		LONG_T,
		FLOAT_T,
		DOUBLE_T,
		DATE_T,
		BLOB_T,
	};
	using attribute_enum_type = std::uint8_t;

	constexpr size_t SEGMENT_BIT_LENGTH = 32;
	constexpr address SEGMENT_SIZE = static_cast<address>(1) << SEGMENT_BIT_LENGTH;
	constexpr size_t MAX_SEG_CAPACITY = 240; // decide by TranslatorEntryPage
	constexpr size_t METADATA_SEG_CAPACITY = 1;
	constexpr size_t BLOB_SEG_CAPACITY = 59;
	constexpr size_t DATA_SEG_CAPACITY = 60;
	constexpr size_t INDEX_SEG_CAPACITY = 40;
	constexpr size_t TEMP_SEG_CAPACITY = MAX_SEG_CAPACITY - METADATA_SEG_CAPACITY - BLOB_SEG_CAPACITY - DATA_SEG_CAPACITY - INDEX_SEG_CAPACITY; // capacity left

	constexpr size_t PAGE_BIT_LENGTH = 12;
	constexpr address PAGE_SIZE = static_cast<address>(1) << PAGE_BIT_LENGTH;

	constexpr address TUPLE_CAPACITY = 0xffff;

	constexpr drive_address FIXED_DRIVE_ENTRY_PAGE = 0;
	constexpr drive_address FIXED_TRANSLATOR_ENTRY_PAGE = FIXED_DRIVE_ENTRY_PAGE + PAGE_SIZE;
	constexpr drive_address FIXED_SIZE = FIXED_TRANSLATOR_ENTRY_PAGE + PAGE_SIZE;

	constexpr drive_address INIT_SIZE = PAGE_SIZE * 0x400;
	constexpr drive_address INIT_SYSTEM_SIZE = PAGE_SIZE * 0;
	constexpr drive_address INIT_USER_SIZE = INIT_SIZE - FIXED_SIZE - INIT_SYSTEM_SIZE;

	constexpr drive_address EXPAND_SIZE = PAGE_SIZE * 1;
	constexpr drive_address EXPAND_SYSTEM_SIZE = PAGE_SIZE * 10;
	constexpr drive_address EXPAND_USER_SIZE = PAGE_SIZE * 100;

	constexpr drive_address SHRINK_SIZE = PAGE_SIZE * 1;
	constexpr drive_address SHRINK_SYSTEM_SIZE = PAGE_SIZE * 1;
	constexpr drive_address SHRINK_USER_SIZE = PAGE_SIZE * 1;

	constexpr size_t LOOKASIDE_SIZE = 1; // TODO: use lookaside to 0x800 when partial load mapping pages;
	constexpr size_t KEEPER_CACHE_TOTAL_SIZE = 0x400;
	constexpr size_t KEEPER_CACHE_LEVEL = 3;
	constexpr size_t KEEPER_CACHE_SIZES[KEEPER_CACHE_LEVEL] = { 0x40, 0xc0, 0x300 };
	// TODO: exception system
}
