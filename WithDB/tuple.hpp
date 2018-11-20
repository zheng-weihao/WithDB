#pragma once

#include "endian_function.hpp"
#include "keeper.hpp"
#include "type_config.hpp"

#include <algorithm>
#include <iterator>
#include <type_traits>
#include <utility>
#include <vector>

namespace db {
	namespace ns::tuple { // TODO: replace other inner ns with this style
		template<typename Iter>
		using enable_if_char_iterator_t = std::enable_if_t<std::is_same_v<typename std::iterator_traits<Iter>::value_type, char>>;

		template<typename Iter>
		using enable_if_random_iterator_t = std::enable_if_t<std::is_same_v<typename std::iterator_traits<Iter>::iterator_category, std::random_access_iterator_tag>>;
	}

	struct piece_entry {
		page_address index;
		page_address begin;
		page_address end;
		bool is_complete;
		bool is_head;
		bool is_free;
		piece_entry(page_address index, page_address begin = PAGE_SIZE, page_address end = PAGE_SIZE, bool is_complete = true, bool is_head = true, bool is_free = false) :
			index(index), begin(begin), end(end), is_complete(is_complete), is_head(is_head), is_free(is_free) {
		}

		page_address size() {
			return end - begin;
		}
	};

	struct tuple_page : virtual_page {
		constexpr static page_address FLAGS_POS = 0;
		constexpr static page_address USED_SIZE_POS = 2;
		constexpr static page_address FRONT_PTR_POS = 4;
		constexpr static page_address BACK_PTR_POS = 6;
		constexpr static page_address HEADER_SIZE = 8;

		constexpr static page_address PIECE_ENTRY_SIZE = 4;
		constexpr static page_address PIECE_ENTRY_INDEX_POS = 0;
		constexpr static page_address PIECE_ENTRY_COMPLETE_BIT = 15;
		constexpr static page_address PIECE_ENTRY_HEAD_BIT = 14;
		constexpr static page_address PIECE_ENTRY_DELETED_BIT = 13;
		constexpr static page_address PIECE_ENTRY_PTR_POS = 2;

		// TODO: flags store other information
		page_address flags;
		page_address used_size;
		page_address front_ptr;
		page_address back_ptr;

		std::vector<piece_entry> piece_table;
	public:
		virtual void load() {
			reactivate();
			while (!pin()) {
			}
			flags = read<page_address>(FLAGS_POS);
			piece_table.clear();
			if (flags) {
				used_size = read<page_address>(USED_SIZE_POS);
				front_ptr = read<page_address>(FRONT_PTR_POS);
				back_ptr = read<page_address>(BACK_PTR_POS);
				auto last = static_cast<page_address>(PAGE_SIZE);
				for (page_address i = HEADER_SIZE; i != front_ptr; i += PIECE_ENTRY_SIZE) {
					auto index = read<page_address>(i + PIECE_ENTRY_INDEX_POS);
					auto ptr = read<page_address>(i + PIECE_ENTRY_PTR_POS);
					piece_table.emplace_back(reset_flags(index, PIECE_ENTRY_DELETED_BIT),
						ptr, last, get_flag(index, PIECE_ENTRY_COMPLETE_BIT),
						get_flag(index, PIECE_ENTRY_HEAD_BIT),
						get_flag(index, PIECE_ENTRY_DELETED_BIT)
					);
					last = ptr;
				}
				order_by_index();
			} else {
				unpin();
				throw std::out_of_range("clean page");
				
			}
			unpin();
		}

		virtual void dump() {
			order_by_position();
			reactivate();
			while (!pin()) {
			}
			if (flags) {
				write(flags, FLAGS_POS);
				write(used_size, USED_SIZE_POS);
				write(front_ptr, FRONT_PTR_POS);
				write(back_ptr, BACK_PTR_POS);
				page_address i = HEADER_SIZE;
				for (auto &entry : piece_table) {
					auto index = entry.index;
					index = set_flag(index, entry.is_complete, PIECE_ENTRY_COMPLETE_BIT);
					index = set_flag(index, entry.is_head, PIECE_ENTRY_DELETED_BIT);
					index = set_flag(index, entry.is_free, PIECE_ENTRY_DELETED_BIT);
					auto ptr = entry.begin;
					write(index, i + PIECE_ENTRY_INDEX_POS);
					write(ptr, i + PIECE_ENTRY_PTR_POS);
					i += PIECE_ENTRY_SIZE;
				}
			}
			unpin();
			order_by_index();
		}

		tuple_page(virtual_page &&origin) : virtual_page(std::move(origin)) {
		}

		void init() {
			flags = 1;
			front_ptr = HEADER_SIZE;
			used_size = HEADER_SIZE;
			back_ptr = PAGE_SIZE;
		}

		void close() {
			dump();
		}

		void order_by_index() {
			std::sort(piece_table.begin(), piece_table.end(), [](const piece_entry &a, const piece_entry &b) {
				return a.index < b.index;
			});
		}

		void order_by_position() {
			std::sort(piece_table.begin(), piece_table.end(), [](const piece_entry &a, const piece_entry &b) {
				return a.begin > b.begin;
			});
		}

		page_address get_free_space(bool fast = true) {
			return fast ? back_ptr - front_ptr : PAGE_SIZE - used_size;
		}

		page_address get_pos(page_address index) {
			piece_entry tmp(index);
			auto iter = std::lower_bound(piece_table.begin(), piece_table.end(), tmp, [](const piece_entry &a, const piece_entry &b) {
				return a.index < b.index;
			});
			if (iter != piece_table.end() && iter->index != index) {
				iter = piece_table.end();
			}
			return static_cast<page_address>(iter - piece_table.begin());
		}

		// [begin, end)
		std::pair<page_address, page_address> get(page_address index) {
			auto pos = get_pos(index);
			if (pos != piece_table.size() && !piece_table[pos].is_free) {
				return std::make_pair(piece_table[pos].begin, piece_table[pos].end);
			} else {
				return std::make_pair(FLAGS_POS, FLAGS_POS);
			}
		}

		// copy without checking outer layer should check
		template<typename Iter,
			ns::tuple::enable_if_char_iterator_t<Iter> * = nullptr
		> void copy_to(Iter out, page_address begin, page_address end) {
			reactivate();
			while (!pin()) {
			}
			for (auto i = begin; i != end; ++i) {
				*out++ = read<char>(i);
			}
			unpin();
		}
		template<typename Iter,
			ns::tuple::enable_if_char_iterator_t<Iter> * = nullptr
		> void copy_from(Iter in, page_address begin, page_address end) {
			reactivate();
			while (!pin()) {
			}
			for (auto i = begin; i != end; ++i) {
				write(*in++, i);
			}
			unpin();
		}

		inline page_address append(page_address size) {
			auto iter = piece_table.begin();
			page_address tmp = 0;
			for (; iter != piece_table.end() && iter->index == tmp; ++iter, ++tmp) {
			}
			back_ptr -= size;
			front_ptr += PIECE_ENTRY_SIZE;
			used_size += size + PIECE_ENTRY_SIZE;
			piece_table.emplace(iter, tmp, back_ptr, back_ptr + size);
			return tmp;
		}

		// return index
		page_address allocate(page_address size, bool fast = true) {
			if (size + PIECE_ENTRY_SIZE > get_free_space(fast)) {
				// TODO: don't use exception to manage control flow
				throw std::out_of_range("no enough space");
			}
			if (!fast && size + PIECE_ENTRY_SIZE > get_free_space()) {
				sweep();
			}
			return append(size);
		}

		void free(page_address index) {
			auto pos = get_pos(index);
			if (pos != piece_table.size() && !piece_table[pos].is_free) {
				piece_table[pos].is_free = true;
				used_size -= static_cast<page_address>(piece_table[pos].size());
			}
		}

		void sweep() {
			order_by_position();
			reactivate();
			while (!pin()) {
			}
			front_ptr = HEADER_SIZE;
			back_ptr = PAGE_SIZE;
			for (auto &entry : piece_table) {
				if (!entry.is_free) {
					front_ptr += PIECE_ENTRY_SIZE;
					auto tmp = entry.end;
					entry.end = back_ptr;
					while (tmp-- > entry.begin) {
						write(read<char>(--back_ptr), tmp);
					}
					entry.begin = back_ptr;
				}
			}
			unpin();
			used_size = PAGE_SIZE - back_ptr + front_ptr;
			piece_table.erase(std::remove_if(piece_table.begin(), piece_table.end(), [](const piece_entry &e) {
				return e.is_free;
			}));
			order_by_index();
		}

		~tuple_page() {
			close();
		}
	};

	// TODO:
	// changable length concept class with random access iterator
	/*template<typename Iter,
		ns::tuple::enable_if_char_iterator_t<Iter> * = nullptr,
		ns::tuple::enable_if_random_iterator_t<Iter> * = nullptr
	>
		struct basic_tuple {
		template<typename Iter>
		basic_tuple(Iter first, Iter last) : first, last) {
		}

		template<typename Type>
		inline Type get(iterator first, iterator last) {
			return get_primitive<Type>(first, last);
		}

		template<>
		inline std::string get<std::string>(iterator first, iterator last) {
			return get_string(first, last);
		}

		template<typename Type>
		Type get(page_address first) {
			return get<Type>(begin() + first, end());
		}


		template<typename Type>
		inline void set(Type value, iterator first, iterator last) {
			set_primitive(value, first, last);
		}

		template<>
		inline void set<std::string>(std::string value, iterator first, iterator last) {
			set_string(value, first, last);
		}

		template<typename Type>
		void set(Type value, page_address first) {
			set(value, begin() + first, end());
		}

		tuple() {
		}
	};*/

	// continuous tuple for building and retrive on one page or copy the whole tuple to in without reference
	using tuple = std::vector<char>;

	class tuple_entry {
		attribute_type_enum type;
		page_address offset = 0;
		page_address size = 0;

	public:
		tuple_entry(attribute_type_enum type, page_address size = 0, page_address offset = 0) : type(type), size(size), offset(offset) {
		}

		inline constexpr auto get_type() const {
			return type;
		}

		inline constexpr void set_type(attribute_type_enum type) {
			this->type = type;
		}

		inline constexpr auto get_offset() const {
			return offset;
		}

		inline constexpr void set_offset(page_address offset) {
			this->offset = offset;
		}

		inline constexpr page_address get_size() const {
			return size;
		}

		inline constexpr auto set_size(page_address size) {
			this->size = size;
		}

		template<typename Type>
		Type read(tuple &t) {
			if (get_size() != sizeof(Type)) {
				// simple type size checking without reflection for efficiency
				// TODO: maybe add RTTI checking in the future
				throw std::runtime_error("[db::tuple_entry::get] get wrong type in getting");
			}
			return read_value<Type>(t.begin() + offset, t.end());
		}

		template<>
		std::string read<std::string>(tuple &t) {
			// TODO: checking condition for robustness
			auto iter = t.begin() + get_offset();
			page_address first = 0, last = 0;
			switch (get_type()) {
			case db::CHAR_T:
			case db::DATE_T:
				return read_value<std::string>(iter, iter + get_size());
			case db::VARCHAR_T:
				first = read_value<page_address>(iter, t.end());
				last = read_value<page_address>(iter + sizeof(page_address), t.end());
				return read_value<std::string>(t.begin() + first, t.begin() + last);
			case db::BLOB_T:
				std::runtime_error("[db::tuple_entry::get] not implemented blob handling");
				return "";
			default:
				throw std::out_of_range("[db::tuple_entry::get] unknown enum type value");
				return "";
			}
		}
	};

	struct tuple_table : std::vector<tuple_entry> {
		page_address fix_size;

		tuple_table(const tuple_table &other) : std::vector<tuple_entry>(other), fix_size(other.fix_size) {
		}

		tuple_table(tuple_table &&other) : std::vector<tuple_entry>(other), fix_size(other.fix_size) {
		}

		tuple_table(std::initializer_list<tuple_entry> init) : std::vector<tuple_entry>(init) {
			format(); // TODO: open align when adding data dictionary
		}

		auto get_fix_size() {
			return fix_size;
		}

		template<typename Type>
		Type get(tuple &t, std::size_t index) {
			return (*this)[index].read<Type>(t);
		}

		void format(bool alignment = false) {
			set_entries_size();
			if (alignment) {
				sort_entries_for_alignment();
			}
			set_entries_offset();
		}

		inline void set_entries_size() {
			for (auto && entry : *this) {
				auto s = map_entry_size(entry.get_type());
				if (s > 0) {
					entry.set_size(static_cast<page_address>(s));
				}
			}
		}

		inline void sort_entries_for_alignment() {
			std::sort(begin(), end(), [](tuple_entry &a, tuple_entry &b) {
				if ((a.get_type() == CHAR_T) ^ (b.get_type() == CHAR_T)) {
					return b.get_type() == CHAR_T;
				} else {
					return a.get_size() > b.get_size();
				}
			});
		}

		inline void set_entries_offset() {
			page_address tmp = 0;
			for (auto && entry : *this) {
				entry.set_offset(tmp);
				auto m = tmp + entry.get_size();
				if (m < tmp) {
					throw std::out_of_range("[db::tuple_table::set_entries_offset] attribles are too long for a single tuple");
				}
				tmp = m;
			}
			fix_size = tmp;
		}

		inline std::size_t map_entry_size(attribute_type_enum type) {
			switch (type) {
			case db::CHAR_T:
				return 0; // user decide
			case db::VARCHAR_T:
				return sizeof(page_address) * 2;
			case db::INT_T:
				return sizeof(int_type);
			case db::LONG_T:
				return sizeof(long_type);
			case db::FLOAT_T:
				return sizeof(float_type);
			case db::DOUBLE_T:
				return sizeof(double_type);
			case db::DATE_T:
				return sizeof(element_type) * 8;
			case db::BLOB_T:
				return sizeof(address);
			case db::DUMMY_T:
				return 0;
			default:
				throw std::out_of_range("[auto_size_map] unknown enum type value");
			}
		}
	};

	// build continuous tuple to save on page
	class tuple_builder {
	private:
		std::shared_ptr<tuple_table> table;

		std::shared_ptr<tuple> current;
		std::vector<bool> is_set;
	public:
		auto get_table() {
			return table;
		}

		void set_table(std::shared_ptr<tuple_table> table) {
			this->table = table;
		}

		void start() {
			if (!table) {
				throw std::runtime_error("[db::tuple_builder::start] detect no tuple_table link");
			}
			current = std::make_shared<tuple>(table->get_fix_size(), 0);
			is_set.clear();
			is_set.resize(table->size(), false);
		}

		template<typename Type>
		void set(Type value, std::size_t index) {
			if (is_set[index]) {
				// TODO: handle duplicate write
				throw std::runtime_error("[db::tuple_builder::set] duplication in building");
			} else {
				is_set[index] = true;
				set_template(value, (*table)[index], *current);
			}
		}

		template<typename Type>
		void set_template(Type value, tuple_entry &entry, tuple &t) {
			if (entry.get_size() != sizeof(Type)) {
				// simple type size checking without reflection for efficiency
				// TODO: maybe add RTTI checking in the future
				throw std::runtime_error("[tuple_builder::set_template] get wrong type in setting");
			}
			write_value(value, t.begin() + entry.get_offset(), t.end());
		}

		template<>
		void set_template<std::string>(std::string s, tuple_entry &entry, tuple &t) {
			if (static_cast<page_address>(s.size()) < s.size()) {
				throw std::runtime_error("[db::tuple_builder::set_template] address overflow");
			}
			switch (entry.get_type()) {
			case db::CHAR_T:
			case db::DATE_T:
				if (s.size() <= entry.get_size()) {
					write_value(s, t.begin() + entry.get_offset(), t.end());
				} else {
					write_value(s.substr(0, entry.get_size()), t.begin() + entry.get_offset(), t.end());
				}
				return;
			case db::VARCHAR_T:
				if (static_cast<page_address>(s.size() + t.size()) < t.size()) {
					throw std::runtime_error("[db::tuple_builder::set_template] address overflow");
				} else {
					auto first = static_cast<page_address>(t.size());
					auto last = static_cast<page_address>(t.size() + s.size());
					write_value(first, t.begin() + entry.get_offset(), t.end());
					write_value(last, t.begin() + entry.get_offset() + sizeof(page_address), t.end());
					t.resize(last);
					write_value(s, t.begin() + first, t.end());
				}
				return;
			case db::BLOB_T:
				std::runtime_error("[db::tuple_builder::set_template]not implemented blob handling");
				return;
			default:
				throw std::out_of_range("[auto_size_map] unknown enum type value");
				return;
			}
		}

		std::shared_ptr<tuple> get() {
			return current;
		}

		void reset() {
			current.reset();
			is_set.clear();
		}
	};
	
}