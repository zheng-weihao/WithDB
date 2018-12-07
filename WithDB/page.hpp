#ifndef __PAGE_HPP__
#define __PAGE_HPP__

#include "endian.hpp"
#include "definitions.hpp"

#include <iterator>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace db {
	namespace ns::page {
		template<typename Iter>
		using enable_if_char_iterator_t = std::enable_if_t<std::is_same_v<typename std::iterator_traits<Iter>::value_type, char>>;

		template<typename Iter>
		using enable_if_random_iterator_t = std::enable_if_t<std::is_same_v<typename std::iterator_traits<Iter>::iterator_category, std::random_access_iterator_tag>>;
	}

	template<typename Iter, std::size_t Size = PAGE_SIZE,
		ns::page::enable_if_char_iterator_t<Iter> * = nullptr,
		ns::page::enable_if_random_iterator_t<Iter> * = nullptr
	>
	struct basic_page {
		using shared_type = std::shared_ptr<std::pair<Iter, Iter>>;
		shared_type shared_pair;

	public:
		inline basic_page(const shared_type &shared_pair) : shared_pair(shared_pair) {
			if (shared_pair && size() > static_cast<std::ptrdiff_t>(Size)) {
				throw std::out_of_range("[basic_page::constructor] space too large for page operation");
			}
		}
		inline basic_page(shared_type &&shared_pair): shared_pair(shared_pair) {
			if (shared_pair && size() > static_cast<std::ptrdiff_t>(Size)) {
				throw std::out_of_range("[basic_page::constructor] space too large for page operation");
			}
		}

		inline basic_page(Iter first, Iter last): basic_page(std::make_shared<std::pair<Iter, Iter>>(std::make_pair(first, last))) {
		}

		inline basic_page(const basic_page &other) : shared_pair(other.shared_pair) {
		}

		inline basic_page(basic_page &&other) : shared_pair(std::move(other.shared_pair)) {
		}

		inline basic_page() {
		}

		inline basic_page &operator=(const basic_page &other) {
			shared_pair = other.shared_pair;
			return *this;
		}

		inline basic_page &operator=(basic_page &&other) {
			shared_pair = std::move(other.shared_pair);
			return *this;
		}

		inline shared_type get_pair_ptr() {
			return shared_pair;
		}

		inline void set_pair_ptr(shared_type &&shared_pair)  {
			if (shared_pair && size() > static_cast<std::ptrdiff_t>(Size)) {
				throw std::out_of_range("[basic_page::constructor] space too large for page operation");
			}
			this->shared_pair = std::move(shared_pair);
		}

		inline void set_pair_ptr(const shared_type &shared_pair) {
			if (shared_pair && size() > static_cast<std::ptrdiff_t>(Size)) {
				throw std::out_of_range("[basic_page::constructor] space too large for page operation");
			}
			this->shared_pair = shared_pair;
		}

		inline void set_pair_ptr(Iter first, Iter last) {
			set_pair_ptr(std::make_shared<std::pair<Iter, Iter>>(std::make_pair(first, last)));
		}

		inline bool is_active() {
			return shared_pair && size() != 0;
		}

		inline void deactivate() {
			shared_pair->second = shared_pair->first;
		}

		inline Iter begin() {
			return shared_pair->first;
		}

		inline Iter end() {
			return shared_pair->second;
		}

		inline std::size_t size() {
			return shared_pair ? end() - begin() : 0;
		}

		template<typename Type>
		inline Type read(page_address first, page_address last) {
			auto b = begin(), e = end();
			if (b == e || last > e - b || first >= last) {
				throw std::out_of_range("[basic_page::read] address fetch error or out of page range");
			}
			return read_value<Type>(b + first, b + last);
		}

		template<typename Type>
		inline Type read(page_address first) {
			auto b = begin(), e = end();
			if (b == e || first >= e - b) {
				throw std::out_of_range("[basic_page::read] address fetch error or out of page range");
			}
			return read<Type>(b + first, e);
		}

		template<typename Type>
		inline void write(Type value, page_address first, page_address last) {
			auto b = begin(), e = end();
			if (b == e || last > e - b || first >= last) {
				throw std::out_of_range("[basic_page::write] address fetch error or out of page range");
			}
			write_value(value, b + first, b + last);
		}

		template<typename Type>
		inline void write(Type value, page_address first) {
			auto b = begin(), e = end();
			if (b == e || first >= e - b) {
				throw std::out_of_range("[basic_page::write] address fetch error or out of page range");
			}
			write_value(value, b + first, e);
		}

		void clear() {
			for (auto iter = begin(); iter != end(); ++iter) {
				*iter = 0;
			}
		}

		virtual void load() {
		}

		virtual void dump() {
		}

		using iterator = Iter;
	};

	using page = basic_page<std::vector<char>::iterator>;
}

#endif // __PAGE_HPP__

