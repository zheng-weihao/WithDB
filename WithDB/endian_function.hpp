#ifndef __ENDIAN_FUNCTION_HPP__
#define __ENDIAN_FUNCTION_HPP__

// basic serialize to and deserialize from bitflow buffer in network order

#include <cstddef>
#include <cstdint>
#include <exception>
#include <iterator>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>

// TODO: portable network order for other operation system using C interface precompiled command
#pragma comment(lib, "wsock32.lib")
#include <winsock2.h>
// eg. linux:
// #include <endian.h>

namespace db {
	namespace endian_ns {
		// is_primitive_size: template contains value that indicates whether Size is 1, 2, 4, 8
		template<std::size_t Size>
		struct is_primitive_size : std::conditional_t<(Size > 0 && Size <= 8 && Size % 2 == 0), is_primitive_size<Size / 2>, std::false_type> {
		};

		template<>
		struct is_primitive_size<1> : std::true_type {
		};

		template<std::size_t Size>
		inline constexpr bool is_primitive_size_v = is_primitive_size<Size>::value;

		// max_primitive_size: template contains value that is the max primitive size equal to or less than Size
		template<std::size_t Size>
		struct max_primitive_size : std::conditional_t<
			is_primitive_size_v<Size>,
			std::integral_constant<std::size_t, Size>,
			std::conditional_t<
			(Size > 8),
			std::integral_constant<std::size_t, 8>,
			max_primitive_size<Size - 1>
		>
		> {
		};

		template<std::size_t Size>
		inline constexpr std::size_t max_primitive_size_v = max_primitive_size<Size>::value;

		// enable template if Type's size is not primitive
		template<typename Type>
		using enable_if_primitive_t = std::enable_if_t<is_primitive_size_v<sizeof(Type)>>;

		// enable template if Type's size is not primitive
		template<typename Type>
		using enable_if_not_primitive_t = std::enable_if_t<!is_primitive_size_v<sizeof(Type)>>;

		// enable template if Type's size equals Size
		template<typename Type, std::size_t Size>
		using enable_if_size_t = std::enable_if_t<sizeof(Type) == Size>;
	}

	// endian_serialize: template function return network order bitflow
	template<typename Type, endian_ns::enable_if_not_primitive_t<Type>* = nullptr>
	inline Type endian_serialize(Type value) {
		// TODO: handle unaligned basic data type
		throw std::runtime_error("[endian_deserialize] Not implemented exception for unaligned basic data type");
		return value;
	}

	template<typename Type8, endian_ns::enable_if_size_t<Type8, 1> * = nullptr>
	inline constexpr std::uint8_t endian_serialize(Type8 value) {
		return *reinterpret_cast<std::uint8_t *>(&value);
	}

	template<typename Type16, endian_ns::enable_if_size_t<Type16, 2> * = nullptr>
	inline constexpr std::uint16_t endian_serialize(Type16 value) {
		return htons(*reinterpret_cast<std::uint16_t *>(&value));
	}

	template<typename Type32, endian_ns::enable_if_size_t<Type32, 4> * = nullptr>
	inline constexpr std::uint32_t endian_serialize(Type32 value) {
		return htonl(*reinterpret_cast<std::uint32_t *>(&value));
	}

	template<typename Type64, endian_ns::enable_if_size_t<Type64, 8> * = nullptr>
	inline constexpr std::uint64_t endian_serialize(Type64 value) {
		return htonll(*reinterpret_cast<std::uint64_t *>(&value));
	}

	// endian_deserialize: template function return host order bitflow
	template<typename Type, endian_ns::enable_if_not_primitive_t<Type> * = nullptr>
	inline Type endian_deserialize(Type value) {
		// TODO: handle unaligned basic data type
		throw std::runtime_error("[endian_deserialize] Not implemented exception for unaligned basic data type");
		return value;
	}

	template<typename Type8, endian_ns::enable_if_size_t<Type8, 1> * = nullptr>
	inline constexpr Type8 endian_deserialize(std::uint8_t value) {
		return *reinterpret_cast<Type8 *>(&value);
	}

	template<typename Type16, endian_ns::enable_if_size_t<Type16, 2> * = nullptr>
	inline constexpr Type16 endian_deserialize(std::uint16_t value) {
		return *reinterpret_cast<Type16 *>(&(value = ntohs(value)));
	}

	template<typename Type32, endian_ns::enable_if_size_t<Type32, 4> * = nullptr>
	inline constexpr Type32 endian_deserialize(std::uint32_t value) {
		return *reinterpret_cast<Type32 *>(&(value = ntohl(value)));
	}

	template<typename Type64, endian_ns::enable_if_size_t<Type64, 8> * = nullptr>
	inline Type64 endian_deserialize(std::uint64_t value) {
		return *reinterpret_cast<Type64 *>(&(value = ntohll(value)));
	}

	namespace endian_ns {
		// enable template if Iter's value type is char type
		template<typename Iter>
		using enable_if_char_iterator_t = std::enable_if_t<
			std::is_same_v<char, typename std::iterator_traits<Iter>::value_type>
		>;

		// enable template if Iter has forword ability
		template<typename Iter>
		using enable_if_forward_iterator_t = std::enable_if_t<
			std::is_base_of_v<std::forward_iterator_tag, typename std::iterator_traits<Iter>::iterator_category>
		>;

		template<typename Type>
		using enable_if_string_type_t = std::enable_if_t<std::is_same_v<std::string, Type>>;

		template<typename Type>
		using enable_if_not_string_type_t = std::enable_if_t<!std::is_same_v<std::string, Type>>;
	}


	// read_value: template function to read basic type and fix structure data
	template<typename Type, typename Iter,
		endian_ns::enable_if_char_iterator_t<Iter> * = nullptr,
		endian_ns::enable_if_forward_iterator_t<Iter> * = nullptr,
		endian_ns::enable_if_not_string_type_t<Type> * = nullptr
	> Type read_value(Iter first, Iter last) {
		Type ret;
		decltype(endian_serialize(ret)) tmp;
		auto ptr = reinterpret_cast<char *>(&tmp);
		std::size_t cnt = 0;
		for (auto iter = first; iter != last && cnt < sizeof(Type); ++iter, ++cnt) {
			*(ptr + cnt) = *iter;
		}
		if (cnt < sizeof(Type)) {
			throw std::out_of_range("[read_value] iterator out of range");
		}
		ret = endian_deserialize<Type>(tmp);
		return ret;
	}

	// read_value: template function to read string type data
	template<typename Type, typename Iter,
		endian_ns::enable_if_char_iterator_t<Iter> * = nullptr,
		endian_ns::enable_if_forward_iterator_t<Iter> * = nullptr,
		endian_ns::enable_if_string_type_t<Type> * = nullptr
	> Type read_value(Iter first, Iter last) {
		std::stringstream sstr;
		for (auto iter = first; iter != last && *iter; ++iter) {
			sstr << *iter;
		}
		return sstr.str();
	}

	// write_value: template function to write basic type and fix structure data
	template<typename Type, typename Iter,
		endian_ns::enable_if_not_string_type_t<Type> * = nullptr,
		endian_ns::enable_if_char_iterator_t<Iter> * = nullptr,
		endian_ns::enable_if_forward_iterator_t<Iter> * = nullptr
	> void write_value(Type value, Iter first, Iter last) {
		auto tmp = endian_serialize(value);
		auto ptr = reinterpret_cast<char *>(&tmp);
		std::size_t cnt = 0;
		for (auto iter = first; iter != last && cnt < sizeof(Type); ++iter, ++cnt) {
			*iter = *(ptr + cnt);
		}
		if (cnt < sizeof(Type)) {
			throw std::out_of_range("[write_value] iterator out of range");
		}
	}

	// write_value: template function to write string type data
	template <typename Type, typename Iter,
		endian_ns::enable_if_string_type_t<Type> * = nullptr,
		endian_ns::enable_if_char_iterator_t<Iter> * = nullptr,
		endian_ns::enable_if_forward_iterator_t<Iter> * = nullptr
	> void write_value(const Type &value, Iter first, Iter last) {
		auto iter = first;
		for (auto ch : value) {
			if (iter != last) {
				*iter = ch;
				++iter;
			} else {
				throw std::out_of_range("[write_value] iterator out of range");
				return;
			}
		}
	}


	template<typename Type, endian_ns::enable_if_primitive_t<Type> * = nullptr>
	inline bool get_flag(Type value, int pos) {
		return value & (1l << pos);
	}

	template<typename Type, endian_ns::enable_if_primitive_t<Type> * = nullptr>
	inline Type set_flag(Type value, bool flag, int pos) {
		if (flag) {
			return static_cast<Type>(value | (1ll << pos));
		} else {
			return static_cast<Type>(value & ~(1ll << pos));
		}
	}

	template<typename Type, endian_ns::enable_if_primitive_t<Type> * = nullptr>
	inline Type reset_flags(Type value, int pos) {
		return static_cast<Type>(value & ((1ll << pos) - 1));
	}

	template<typename Type, endian_ns::enable_if_primitive_t<Type> * = nullptr>
	inline Type reset_flags(Type value, int first, int last) {
		if (last == sizeof(Type)) {
			return reset_flags(value, first);
		} else {
			return static_cast<Type>(value & ~((1ll << last) - (1ll << first)));
		}
	}
}

#endif // __ENDIAN_FUNCTION_HPP__
