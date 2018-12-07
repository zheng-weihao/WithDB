#pragma once

// basic serialize to and deserialize from bitflow buffer in network order

#include "definitions.hpp"

#include <cstddef>
#include <cstdint>
#include <iterator>
#include <string>
#include <type_traits>

// TODO: portable network order for other operation system using C interface precompiled command
// TODO: find out why pragma comment works and why inline template funtion compiling will fail in some cases without this order
#pragma comment(lib, "wsock32.lib")
#include <winsock2.h>

namespace db {
	namespace ns::endian {
		template<std::size_t Size>
		inline uint_t<Size> encode_port(uint_t<Size> value) { return value; }
		template<>
		inline uint_t<2> encode_port<2>(uint_t<2> value) { return htons(value); }
		template<>
		inline uint_t<4> encode_port<4>(uint_t<4> value) { return htonl(value); }
		template<>
		inline uint_t<8> encode_port<8>(uint_t<8> value) { return htonll(value); }

		template<std::size_t Size>
		inline uint_t<Size> decode_port(uint_t<Size> value) { return value; }
		template<>
		inline uint_t<2> decode_port<2>(uint_t<2> value) { return ntohs(value); }
		template<>
		inline uint_t<4> decode_port<4>(uint_t<4> value) { return ntohl(value); }
		template<>
		inline uint_t<8> decode_port<8>(uint_t<8> value) { return ntohll(value); }

		// check whether type is arithmetic basic and right size for portable encode/decode
		template<typename Type>
		using check_network_order = std::conditional_t<std::is_arithmetic_v<Type>, uint<sizeof(Type)>, std::enable_if<false>>;
		template<typename Type>
		using check_network_order_t = typename check_network_order<Type>::type;
		template<typename Type>
		using check_not_network_order_t = check_not_t<check_network_order<Type>>;

		// check if Iter's value type is char type and has forword ability
		template<typename Iter>
		using check_iterator_char = std::enable_if<std::is_same_v<char, typename std::iterator_traits<Iter>::value_type>>;
		template<typename Iter>
		using check_iterator_forward = std::enable_if<std::is_base_of_v<std::forward_iterator_tag, typename std::iterator_traits<Iter>::iterator_category>>;
		template<typename Iter>
		using check_iterator_t = std::void_t<typename check_iterator_forward<Iter>::type, typename check_iterator_char<Iter>::type>;

		// check string type
		template<typename Type>
		using check_string = std::enable_if<std::is_base_of_v<std::string, Type>>;
		template<typename Type>
		using check_string_t = typename check_string<Type>::type;
		template<typename Type>
		using check_not_string_t = check_not_t<check_string<Type>>;
	}

	template<typename Type>
	using network_t = ns::endian::check_network_order_t<Type>;

	// DESIGN:
	// encode-decode are template funtions focusing on using the system network order function (handling portability in encode_port-decode_port)
	// so they are limited to network order allowed type only (structures might have a different arrangement in different system even fits the network lib function size)

	// encode interface to use, host to network order
	template<typename Type>
	inline network_t<Type> encode(Type value) { // arithmetic type so that references are useless
		return ns::endian::encode_port<sizeof(Type)>(reinterpret_cast<network_t<Type> &>(value));
	}

	// decode interface to use, network to host order
	template<typename Type>
	inline Type decode(network_t<Type> value) {
		auto tmp = ns::endian::decode_port<sizeof(Type)>(value);
		return reinterpret_cast<Type &>(tmp);
	}

	// DESIGN:
	// read-write means get-set from a changable size iterator range (char forward is required)
	// read-write works like load-dump a lot, but load-dump are originally designed for page to auto load-dump data from the mapping position, so load-dump(first, last) is an optional for class
	// read-write member funtions are also different from these because they are the helper class to implement the load-dump using read-write, they focus on managing objects' inside members and don't use iterator as parameters

	// read value for network_type from bit order,
	// enable reading any size (exclude size will be abandoned)
	// put the result in the highter bits when input bits are not enough, and set the lower bits to 0
	template<typename Type, typename Iter
		, ns::endian::check_network_order_t<Type> * = nullptr
		, ns::endian::check_iterator_t<Iter> * = nullptr
	> inline void read(Type &ref, Iter first, Iter last) {
		network_t<Type> buffer = 0;
		std::size_t i = 0;
		auto ptr = reinterpret_cast<char *>(&buffer);
		for (auto iter = first; iter != last && i != sizeof(Type); ++iter, ++i) {
			ptr[i] = *iter;
		}
		ref = decode<Type>(buffer);
	}

	// read value for string, cover data from the beginning of the string
	// clear all the char in ref and write input from first to last without '\0' adding
	// only used in char_t, varchar_t and other string data types in database defination
	template<typename Iter
		, ns::endian::check_iterator_t<Iter> * = nullptr
	> inline void read(std::string &ref, Iter first, Iter last) {
		ref.clear();
		for (auto iter = first; iter != last; ++iter) {
			ref.push_back(*iter);
		}
	}

	// read value for other types, call the type defined load function to load data
	template<typename Type, typename Iter
		, ns::endian::check_not_network_order_t<Type> * = nullptr
		, ns::endian::check_not_string_t<Type> * = nullptr
		, ns::endian::check_iterator_t<Iter> * = nullptr
	> inline void read(Type &ref, Iter first, Iter last) {
		ref.load(first, last);
	}

	// TODO: import other template params to import initialize list might solve the default constructor limit like emplace
	// return value directly without reference (require default constructor for other objects)
	template<typename Type, typename Iter
		, ns::endian::check_iterator_t<Iter> * = nullptr
	> inline Type read(Iter first, Iter last) {
		Type tmp;
		read(tmp, first, last);
		return tmp;
	}

	// write value for network_type to network bit order, from the higher bits of the value
	// lower bits will be abandoned if the output iterator reaches the last
	// stop when output sizeof(Type)
	template<typename Type, typename Iter
		, ns::endian::check_network_order_t<Type> * = nullptr
		, ns::endian::check_iterator_t<Iter> * = nullptr
	> inline void write(Type value, Iter first, Iter last) {
		auto buffer = encode(value);
		std::size_t cnt = 0;
		auto ptr = reinterpret_cast<char *>(&buffer);
		for (auto iter = first; iter != last && cnt != sizeof(Type); ++iter, ++cnt) {
			*iter = ptr[cnt];
		}
	}

	// write value for string, use const reference to avoid copy
	// will add '\0' to the end of the string if output iterator doesn't reach the last
	// TODO: need a better consideration about this feature, I think it might cause confusion for further use because when string contains '\0' it will not stop writing
	template<typename Iter
		, ns::endian::check_iterator_t<Iter> * = nullptr
	> inline void write(const std::string &ref, Iter first, Iter last) {
		for (auto iter = ref.begin(); iter != ref.end() && first != last; ++iter, ++first) {
			*first = *iter;
		}
		if (first != last) {
			*first = '\0';
		}
	}

	// read value for other types, call the dump function to load data at head
	template<typename Type, typename Iter
		, ns::endian::check_not_network_order_t<Type> * = nullptr
		, ns::endian::check_not_string_t<Type> * = nullptr
		, ns::endian::check_iterator_t<Iter> * = nullptr
	> inline void write(const Type &ref, Iter first, Iter last) {
		ref.dump(first, last);
	}
}
