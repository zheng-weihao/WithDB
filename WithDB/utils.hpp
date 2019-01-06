#pragma once

#include "definitions.hpp"

#include <chrono>

namespace db {
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

	// TODO: use emplace to replace
	template<typename Type
		, ns::utils::check_arithmetic_t<Type> * = nullptr
	> inline Type defaultValue() {
		return static_cast<Type>(0);
	}

	template<typename Type
		, ns::utils::check_not_arithmetic_t<Type> * = nullptr
	> inline Type defaultValue() {
		return Type();
	}

	namespace ns::utils {
		template<typename Type>
		using check_integral = std::enable_if<std::is_integral_v<Type>>;
		template<typename Type>
		using check_integral_t = typename check_integral<Type>::type;
		template<typename Type>
		using check_not_integral_t = check_not_t<check_integral<Type>>;
	}

	template<typename Type, ns::utils::check_integral_t<Type> * = nullptr>
	inline bool getFlag(Type value, int pos) {
		return value & (1l << pos);
	}

	template<typename Type, ns::utils::check_integral_t<Type> * = nullptr>
	inline Type setFlag(Type value, bool flag, int pos) {
		if (flag) {
			return static_cast<Type>(value | (1ll << pos));
		} else {
			return static_cast<Type>(value & ~(1ll << pos));
		}
	}

	template<typename Type, ns::utils::check_integral_t<Type> * = nullptr>
	inline Type resetFlags(Type value, int pos) {
		return static_cast<Type>(value & ((1ll << pos) - 1));
	}

	template<typename Type, ns::utils::check_integral_t<Type> * = nullptr>
	inline Type resetFlags(Type value, int first, int last) {
		if (last == sizeof(Type)) {
			return resetFlags(value, first);
		} else {
			return static_cast<Type>(value & ~((1ll << last) - (1ll << first)));
		}
	}
}
