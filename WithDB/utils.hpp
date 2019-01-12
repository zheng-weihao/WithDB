#pragma once

#include "definitions.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>

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

	inline timestamp current() {
		return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	}

	namespace ns::utils {
		template<typename Type>
		using check_arithmetic = std::enable_if<std::is_arithmetic_v<Type>>;
		template<typename Type>
		using check_arithmetic_t = typename check_arithmetic<Type>::type;
		template<typename Type>
		using check_not_arithmetic_t = check_not_t<check_arithmetic<Type>>;
	}

	template<typename Type
		, ns::utils::check_arithmetic_t<Type> * = nullptr
	> inline Type getInstance(Type value = static_cast<Type>(0)) {
		return value;
	}

	template<typename Type
		, ns::utils::check_not_arithmetic_t<Type> * = nullptr
		, typename... Args
	> inline Type getInstance(const Args &... args) {
		return Type(args...);
	}

	template<typename Type
		, ns::utils::check_arithmetic_t<Type> * = nullptr
	> inline string toString(Type v) {
		return std::to_string(v);
	}

	inline string toString(const char *v) {
		return string(v);
	}

	template<typename Type
		, ns::utils::check_not_arithmetic_t<Type> * = nullptr
	> inline string toString(const Type &v) {
		return v.toString();
	}

	constexpr size_t getMask(size_t pos) {
		return 1ull << pos;
	}
	constexpr size_t getMask(size_t first, size_t last) {
		return (1ull << last) - (1ull << first);
	}

	template<typename Integral>
	constexpr bool getFlag(Integral flags, size_t pos) {
		return static_cast<std::make_unsigned_t<Integral>>(flags) & getMask(pos);
	}
	template<typename Integral>
	constexpr Integral getFlag(Integral flags, size_t first, size_t last) {
		return (static_cast<std::make_unsigned_t<Integral>>(flags) & getMask(first, last)) >> first;
	}

	template<typename Integral>
	constexpr Integral setFlag(Integral flags, bool value, size_t pos) {
		auto m = getMask(pos);
		auto f = static_cast<std::make_unsigned_t<Integral>>(flags);
		return static_cast<Integral>(value ? (f | m) : (f & ~m));
	}

	template<typename Integral>
	constexpr Integral setFlag(Integral flags, size_t value, size_t first, size_t last) {
		auto m = getMask(first, last);
		auto f = static_cast<std::make_unsigned_t<Integral>>(flags);
		return static_cast<Integral>((f & ~m) | ((value << first) & m));
	}

	template<typename Integral>
	constexpr Integral resetFlag(Integral flags, size_t first, size_t last = sizeof(size_t)) {
		auto m = getMask(first, last);
		auto f = static_cast<std::make_unsigned_t<Integral>>(flags);
		return static_cast<Integral>(f & ~m);
	}

	// static function overload set for data basic types' read write and dynamic type checking
	struct BasicTypes {
		// default (other type) read without checking type, unsigned/int16/int8 or other fixed length type (object pointer ...)
		// simply reinterpret the pointer (sequence data type should handle by upper class)
		template<typename Value, typename... Args>
		inline static bool read(Value &value, element_t *ptr, const Args&... args) {
			value = *reinterpret_cast<Value *>(ptr);
			return true;
		}

		inline static bool read(int_t &value, element_t *ptr, type_enum type = INT_T) {
			return type == INT_T ? (value = *reinterpret_cast<int_t *>(ptr), true) : false;
		}

		inline static bool read(long_t &value, element_t *ptr, type_enum type = LONG_T) {
			return type == LONG_T ? (value = *reinterpret_cast<long_t *>(ptr), true) : false;
		}

		inline static bool read(float_t &value, element_t *ptr, type_enum type = FLOAT_T) {
			return type == FLOAT_T ? (value = *reinterpret_cast<float_t *>(ptr), true) : false;
		}

		inline static bool read(double_t &value, element_t *ptr, type_enum type = DOUBLE_T) {
			return type == DOUBLE_T ? (value = *reinterpret_cast<double_t *>(ptr), true) : false;
		}

		// need a copy for origin value, ptr change to point to end of the string(the end of normal string or the null position)
		// end is at least one element after null terminate
		inline static bool read(element_t *&ptr, element_t *first, element_t *last, type_enum type = CHAR_T) {
			switch (type) {
			case CHAR_T:
			case VARCHAR_T:
			case DATE_T:
				std::copy(first, last, ptr);
				ptr += last - first;
				break;
			case NTBS_T:
			{
				for (; first != last; ++first) { // will not append '\0' if reach src' end
					if (*ptr = *first) { // append '\0' if '\0' is found in src
						++ptr;
					} else {
						break; 
					}
				}
				if (first == last) {
					return false;
				}
				break;
			}
			default:
				return false;;
			}
			return true;
		}

		inline static bool read(string &value, element_t *first, element_t *last, type_enum type = CHAR_T) {
			switch (type) {
			case CHAR_T:
			case VARCHAR_T:
			case DATE_T:
				value.resize(last - first);
				std::copy(first, last, value.begin());
				break;
			case NTBS_T:
			{
				size_t len = 0;
				for (auto ptr = first; ptr != last && *ptr; ++ptr, ++len) {
				}
				if (len == last - first) {
					return false;
				}
				value.resize(len);
				std::copy_n(first, len, value.begin());
				break;
			}
			default:
				return false;;
			}
			return true;
		}

		template<typename Value, typename... Args>
		inline static Value get(const Args&... args) {
			Value v = getInstance<Value>();
			if (read(v, args...)) {
				return v;
			} else {
				throw std::runtime_error("[BasicTypes::read]");
			}
		}

		// default (other type) read without checking type, unsigned/int16/int8 or other fixed length type (object pointer ...)
		// simply reinterpret the pointer (sequence data type should handle by upper class)
		template<typename Value, typename... Args>
		inline static bool write(const Value &value, element_t *ptr, const Args&... args) {
			*reinterpret_cast<Value *>(ptr) = value;
			return true;
		}
		
		inline static bool write(int_t value, element_t *ptr, type_enum type = INT_T) {
			return type == INT_T ? (*reinterpret_cast<int_t *>(ptr) = value, true) : false;
		}

		inline static bool write(long_t value, element_t *ptr, type_enum type = LONG_T) {
			return type == LONG_T ? (*reinterpret_cast<long_t *>(ptr) = value, true) : false;
		}

		inline static bool write(float_t value, element_t *ptr, type_enum type = FLOAT_T) {
			return type == FLOAT_T ? (*reinterpret_cast<float_t *>(ptr) = value, true) : false;
		}

		inline static bool write(double_t value, element_t *ptr, type_enum type = DOUBLE_T) {
			return type == DOUBLE_T ? (*reinterpret_cast<double_t *>(ptr) = value, true) : false;
		}

		inline static bool write(element_t *value, element_t *first, element_t *last, type_enum type = CHAR_T) {
			switch (type) {
			case CHAR_T:
			case VARCHAR_T:
			case DATE_T:
				std::copy_n(value, last - first, first);
				break;
			case NTBS_T:
			{
				for (; first != last; ++first) { // will not append '\0' if reach src' end
					if (*first = *value) { // append '\0' if '\0' is found in src
						++value;
					} else {
						break;
					}
				}
				if (first == last) {
					return false;
				}
				break;
			}
			default:
				return false;;
			}
			return true;
		}

		inline static bool write(const string &value, element_t *first, element_t *last, type_enum type = CHAR_T) {
			switch (type) {
			case CHAR_T:
			case VARCHAR_T:
			case DATE_T:
			{
				auto size = value.size(), destSize = static_cast<size_t>(last - first);
				if (destSize < size) {
					return false;
				}
				std::copy(value.begin(), value.end(), first);
				if (destSize > size) {
					std::fill(first + size, last, '\0');
				}
				break;
			}	
			case NTBS_T:
			{
				auto iter = value.begin();
				for (; first != last; ++first) { // will not append '\0' if reach src' last
					if (*first = *iter) { // append '\0' if '\0' is found in src
						++iter;
					} else {
						break;
					}
				}
				if (first == last) {
					return false;
				}
				break;
			}
			default:
				return false;;
			}
			return true;
		}
	};
}
