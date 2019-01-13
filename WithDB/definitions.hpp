#pragma once

#include <cstddef>
#include <cstdint>
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
	// database size type in indexing
	using size_t = std::size_t;
	// database used string type
	using string = std::string;
	
	// attribute type in memory
	using element_t = char;
	using char_t = std::string;
	using varchar_t = std::string;
	using date_t = std::string;
	using int_t = std::int32_t;
	using long_t = std::int64_t;
	using float_t = float;
	using double_t = double;

	// timestamp type for MRU and other statistic
	using timestamp = std::int64_t;

	// address type with usage
	using address = std::uint64_t;
	using drive_address = std::uint64_t;
	using cache_address = std::uint32_t; // useless, TODO: discuss about pointer swizzling
	using page_address = std::uint16_t;

	// attribute enum for naive RTTI
	enum type_enum {
		DUMMY_T,
		CHAR_T, // can store '\0' in the string
		VARCHAR_T, // can store '\0' in the string
		INT_T,
		LONG_T,
		FLOAT_T,
		DOUBLE_T,
		DATE_T, // store date as 8 char element (char)
		LOB_T,
		BLOB_T,
		CLOB_T,
		NTBS_T, // null-terminate byte string (used in literal, ensure '\0' is not in the string and will find '\0' certainly at the end of string), other has 2 bound
		ATTRIBUTE_T, // claim it is a attribute
		RELATION_T, // claim it is a relation
	};

	enum segment_enum {
		DUMMY_SEG,
		METADATA_SEG,
		BLOB_SEG,
		DATA_SEG,
		INDEX_SEG,
		TEMP_SEG,
	};

	constexpr size_t MAX_STRING_SIZE = 0xffff;

	// unchangable but important literals
	constexpr address TUPLE_CAPACITY = 0xffff;

	constexpr size_t PAGE_BIT_LENGTH = 12;
	constexpr address PAGE_SIZE = 1ll << PAGE_BIT_LENGTH;

	constexpr size_t SEGMENT_BIT_LENGTH = 32;
	constexpr address SEGMENT_SIZE = 1ll << SEGMENT_BIT_LENGTH;

	constexpr address NULL_ADDRESS = 0;

	// changable literals
	constexpr drive_address FIXED_DRIVE_ENTRY_PAGE = 0;
	constexpr drive_address FIXED_TRANSLATOR_ENTRY_PAGE = FIXED_DRIVE_ENTRY_PAGE + PAGE_SIZE;
	constexpr drive_address FIXED_SIZE = FIXED_TRANSLATOR_ENTRY_PAGE + PAGE_SIZE;

	constexpr drive_address INIT_SYSTEM_SIZE = PAGE_SIZE * 0;
	constexpr drive_address INIT_USER_SIZE = PAGE_SIZE * 0;
	constexpr drive_address INIT_SIZE = FIXED_SIZE + INIT_SYSTEM_SIZE + INIT_USER_SIZE;

	constexpr drive_address EXPAND_SIZE = PAGE_SIZE * 1;
	constexpr drive_address EXPAND_SYSTEM_SIZE = PAGE_SIZE * 1;
	constexpr drive_address EXPAND_USER_SIZE = PAGE_SIZE * 0x200;

	constexpr drive_address SHRINK_SIZE = PAGE_SIZE * 1;
	constexpr drive_address SHRINK_SYSTEM_SIZE = PAGE_SIZE * 1;
	constexpr drive_address SHRINK_USER_SIZE = PAGE_SIZE * 1;

	constexpr size_t MAX_SEG_CAPACITY = 240; // max decide by TranslatorEntryPage structure
	constexpr address DATABASE_CAPACITY = MAX_SEG_CAPACITY * SEGMENT_SIZE;

	constexpr size_t METADATA_SEG_CAPACITY = 1;
	constexpr address METADATA_SEG_BEGIN = 0;
	constexpr address METADATA_SEG_END = METADATA_SEG_BEGIN + METADATA_SEG_CAPACITY * SEGMENT_SIZE;

	constexpr size_t BLOB_SEG_CAPACITY = 59;
	constexpr address BLOB_SEG_BEGIN = METADATA_SEG_END;
	constexpr address BLOB_SEG_END = BLOB_SEG_BEGIN + BLOB_SEG_CAPACITY * SEGMENT_SIZE;

	constexpr size_t DATA_SEG_CAPACITY = 60;
	constexpr address DATA_SEG_BEGIN = BLOB_SEG_END;
	constexpr address DATA_SEG_END = DATA_SEG_BEGIN + DATA_SEG_CAPACITY * SEGMENT_SIZE;

	constexpr size_t INDEX_SEG_CAPACITY = 40;
	constexpr address INDEX_SEG_BEGIN = DATA_SEG_END;
	constexpr address INDEX_SEG_END = INDEX_SEG_BEGIN + INDEX_SEG_CAPACITY * SEGMENT_SIZE;

	constexpr size_t TEMP_SEG_CAPACITY = MAX_SEG_CAPACITY - METADATA_SEG_CAPACITY - BLOB_SEG_CAPACITY - DATA_SEG_CAPACITY - INDEX_SEG_CAPACITY; // capacity left
	constexpr address TEMP_SEG_BEGIN = INDEX_SEG_END;
	constexpr address TEMP_SEG_END = DATABASE_CAPACITY;

	constexpr size_t TRANSLATOR_LOOKASIDE_SIZE = 0; // lookaside closed TODO: use lookaside to 0x800 when partial load mapping pages;
	
	constexpr size_t KEEPER_CACHE_LEVEL = 3;
	constexpr size_t KEEPER_CACHE_SIZES[KEEPER_CACHE_LEVEL] = { 0x40, 0xc0, 0x300 };
	
	constexpr size_t getCacheLevel(segment_enum seg) {
		switch (seg) {
		case db::METADATA_SEG:
			return 0;
		case db::BLOB_SEG:
			return 1;
		case db::DATA_SEG:
			return 2;
		case db::INDEX_SEG:
			return 1;
		case db::TEMP_SEG:
			return 2;
		default:
			return KEEPER_CACHE_LEVEL;
		}
	}

	// TODO: exception system
}
