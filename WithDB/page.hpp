#pragma once

#include "cache.hpp"
#include "definitions.hpp"
#include "endian.hpp"

#include <iterator>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace db {
	namespace ns::page {
		template<typename Type>
		using iterator_t = std::iterator_traits<typename Type::iterator>;

		template<typename Type>
		using check_char = std::enable_if<std::is_same_v<typename iterator_t<Type>::value_type, char>>;

		template<typename Type>
		using check_random_access = std::enable_if<std::is_base_of_v<std::random_access_iterator_tag, typename iterator_t<Type>::iterator_category>>;

		template<typename Type>
		using check_container = std::void_t<typename check_char<Type>::type, typename check_random_access<Type>::type>;
	}

	// DESIGN:
	// the most important function in page is the virtual load-dump for derived class, which allow the auto load and write back
	// when page range can be smaller than page size by design, drive will achieve partial load-dump in this situation
	// derived classes should override load-dump carefully if they want this feature
	// page itself doesn't contain db address information, and will not auto writeback or auto reactivate
	// page stores index from the container, generate and use iterator to handle data
	// page has active status, size == 0 means deactive and can't be used unless activate by setting new container range
	// using basic_ because page's almostly used in one format
	// TODO: constructor noexcept and other constructor
	// all the container should have sequence structure
	// page can be partial load but should never partial dump
	template<typename Container, typename Container::size_type Size = PAGE_SIZE
		, ns::page::check_container<Container> * = nullptr
	> struct BasicPage {
		using container_type = Container;
		using value_type = typename Container::value_type;
		using size_type = typename Container::size_type;
		using iterator = typename Container::iterator;
		constexpr static auto page_size = Size;
		// TODO: maybe other types defined by container

		Container &_container;
		size_type _begin, _end;

	public:
		inline BasicPage(Container &container, iterator first, iterator last) : _container(container) {
			activate(first, last);
		}
		inline BasicPage(Container &container, iterator first) : BasicPage(container, first, Size < container.end() - first ? first + Size : container.end()) {
		}
		inline BasicPage(Container &container, size_type first, size_type last) : BasicPage(container, container.begin() + first, container.begin() + last) {
		}
		inline BasicPage(Container &container, size_type first) : BasicPage(container, container.begin() + first) {
		}
		inline BasicPage(Container &container) : _container(container), _begin(0), _end(0) {
		}

		inline BasicPage(const BasicPage &other) : BasicPage(other._container, other.begin(), other.end()) {
		}
		inline BasicPage(BasicPage &&other) : BasicPage(other._container, other.begin(), other.end()) {
			other.deactivate();
		}

		inline BasicPage &operator=(const BasicPage &other) {
			if (&_container == &other._container) {
				activate(other.begin(), other.end());
			} else {
				throw std::runtime_error("[BasicPage::operator=]");
			}
			return *this;
		}

		inline BasicPage &operator=(BasicPage &&other) {
			if (&_container == &other._container) {
				activate(other.begin(), other.end());
				other.deactivate();
			} else {
				throw std::runtime_error("[BasicPage::operator=]");
			}
			return *this;
		}

		inline size_type size() { return _end - _begin; }

		inline iterator begin() const { return _container.begin() + _begin; }

		inline iterator end() const { return _container.begin() + _end; }

		inline value_type *data() { return _container.data() + _begin; }

		inline bool isActive() { return _end != 0; }

		inline void activate(iterator first, iterator last) {
			if (last < first || page_size < last - first) {
				throw std::runtime_error("[BasicPage::activate]");
			}
			_begin = first - _container.begin();
			_end = last - _container.begin();
		}

		// TODO: this code style is better for debugging, not know anything about its efficiency, include code in the constructor
		inline void activate(size_type first, size_type last) { activate(_container.begin() + first, _container.begin() + last); }

		inline void deactivate() { _end = _begin = 0; }

		inline void clear() { std::fill(begin(), end(), '\0'); }

		// read-write wrapper for read-write in endian.hpp, using page_address is safer with fixed container
		// only deal with read small, so return new value directly without reference // TODO: can we add the function read with reference?
		template<typename Type>
		inline Type read(size_type first, size_type last) {
			if (first < last && last <= size()) {
				return db::read<Type>(begin() + first, begin() + last); // access the read funtion in endian.hpp
			} else {
				throw std::runtime_error("[page::read]");
			}
		}
		template<typename Value>
		inline Value read(size_type pos) {
			return read<Value>(pos, size());
		}

		template<typename Value>
		inline void write(const Value &ref, size_type first, size_type last) {
			if (first < last && last <= size()) {
				db::write(ref, begin() + first, begin() + last); // access the read funtion in endian.hpp
			} else {
				throw std::runtime_error("[page::write]");
			}
		}
		template<typename Value>
		inline void write(const Value &ref, size_type pos) {
			write(ref, pos, size());
		}

		// load-dump function for sync data base on the basic page with page memory in container
		virtual bool load() {
			return true;
		}

		virtual bool dump() {
			return true;
		}
	};

	namespace ns::page {
		template<typename Base, typename Derived>
		using check_base = std::enable_if<std::is_base_of_v<Base, Derived>>;
		template<typename Page, typename Derived>
		using check_base_t = typename check_base<Page, Derived>::type;
	}
	
	// DESIGN:
	// partial specialization for the BasicPage
	// container inside to hold page data, we might not implement the scenario that multi-Cache work on the same container's different part
	// but this design allows flexible Cache, and simplify the code in higher level
	// Cache know page's address information but don't know how they are organized
	// hold page shared pointer structure to load-dump polymorphically and a simple method to tell kicked page user the inactivated page status
	// Cache::operator() is a template now to return derived classes' shared_ptr of BasicPage, Cache will share the same object but cast into BasicPage ptr
	// insert-erase are private function just for the convenience, so they don't need to keep the same interface
	// partial load should be read-only, changes on partial load data will be discard
	template<typename Address, typename Container, typename Container::size_type Size, typename Handler, typename Core>
	struct Cache<Address, BasicPage<Container, Size>, Handler, Core> : Core {
		using page_type = BasicPage<Container, Size>;
		using container_type = Container;
		using size_type = typename Container::size_type;
		constexpr static auto page_size = Size;
		
		using super = Core;
		using address_type = Address;
		using value_type = page_type;
		using handler_type = Handler; // using page_type to avoid reference to reference in insert and erase
		using core_type = Core;
		
		Handler &_handler;
		Container _container;
		std::vector<std::shared_ptr<page_type>> _ptrs; // because ptrs are full in most of the status, init and destroy in unordered_map is not appropriate
		std::unordered_map<Address, size_t> _indexes; // record mapping between address and ptrs index

	public:
		inline Cache(Handler &handler, size_t capacity = 0) : super(capacity), _handler(handler), _container(capacity * page_size), _ptrs(capacity) {
		}

		inline Cache(const Cache &other) : super(other), _handler(other._handler), _container(other._container), _ptrs(other._ptrs), _indexes(other._indexes) {
		}

		inline Cache(Cache &&other) : super(std::move(other)), _handler(other._handler), _container(std::move(other._container)), _ptrs(std::move(other._ptrs)), _indexes(std::move(other._indexes)) {
			super::close();
		}

		inline ~Cache() {
			close();
		}

		inline void open(size_t capacity) {
			if (super::isOpen()) {
				throw std::runtime_error("[Cache::open]");
			}
			super::open(capacity);
			_container.resize(capacity * page_size); // TODO: use naive_contianer's vector function, should be discard
			_ptrs.resize(capacity);
		}

		inline void close() {
			if (!super::isOpen()) {
				return;
			}
			super::close();
			for (auto &i : _indexes) {
				bool result = _handler.onErase(i.first, *_ptrs[i.second]);
				if (!result) {
					throw std::runtime_error("[Cache::close]");
				}
			}
			_indexes.clear();
			_ptrs.clear();
			_container.clear(); // TODO: use naive_contianer's vector function, should be discard
		}

		// these hit-insert-erase is not applied on ptrs, because i don't want them to know about the fecth return type information to invoke more template
		// take index to insert on choosen position
		inline size_t hit(Address addr) {
			auto iter = _indexes.find(addr);
			if (iter == _indexes.end()) {
				return _ptrs.size();
			}
			auto index = iter->second;
			auto flag = super::hit(addr) && _handler.onHit(addr, *_ptrs[index]);
			return flag ? index : _ptrs.size();
		}

		inline bool insert(Address addr, size_t index) {
			auto &ptr = _ptrs[index];
			if (!ptr) {
				return false;
			}
			bool flag = _handler.onInsert(addr, *ptr);
			if (flag) {
				_indexes.insert(std::make_pair(addr, index));
			}
			return flag;
		}

		// return index for the addr, else return size of ptrs if fail
		inline size_t erase(Address addr) {
			auto iter = _indexes.find(addr);
			if (iter == _indexes.end()) {
				return _ptrs.size();
			}
			auto index = iter->second;
			auto &ptr = _ptrs[index];
			auto flag = _handler.onErase(addr, *ptr);
			
			if (flag) {
				ptr->deactivate();
				ptr.reset();
				_indexes.erase(iter);
			}
			return flag ? index : _ptrs.size();
		}

		template<typename Type
			, ns::page::check_base_t<page_type, Type> * = nullptr
			, typename... Args
		> inline std::shared_ptr<Type> fetch(Address addr, Address length, bool &flag, Args&&... args) {
			flag = true;
			auto index = hit(addr);
			if (index != _ptrs.size()) {
				// TODO: I think static cast is enough in this case (mostly Cache only contains the same type of derived page), and dynamic cast might limit our ability, need a better consideration
				return std::static_pointer_cast<Type>(_ptrs[index]);
			}
			auto res = super::replace(addr);
			if (res == addr) {
				for (index = 0; index != _ptrs.size(); ++index) {
					if (!_ptrs[index]) {
						break;
					}
				}
			} else {
				index = erase(res);
			}
			if (index == _ptrs.size()) {
				flag = false;
				return nullptr;
			}
			auto ret = std::make_shared<Type>(_container, index * page_size, index * page_size + length, args...); // TODO: less generator arguments
			_ptrs[index] = std::static_pointer_cast<page_type>(ret);
			flag = insert(addr, index);
			if (flag) {
				flag = super::insert(addr); // add to replace when success
			} 
			if (flag) {
				return ret;
			} else {
				_indexes.erase(addr);
				_ptrs[index].reset();
				return nullptr;
			}
		}
		
		template<typename Type
			, ns::page::check_base_t<page_type, Type> * = nullptr
		> inline std::shared_ptr<Type> fetch(Address addr, Address length = static_cast<Address>(page_size)) {
			bool flag;
			auto tmp = fetch<Type>(addr, length, flag);
			if (!flag) {
				throw std::runtime_error("[Cache::fetch]");
			}
			return tmp;
		}

		inline bool discard(Address addr) {
			if (!super::erase(addr)) {
				return false;
			}
			auto result = erase(addr);
			return result != _ptrs.size();
		}
	};

	// DESIGN:
	// page actual owner in memory, need to be optimized for higher performance and more feature

	// TODO: a lot to do with container, mmap(slow start up), flexible memory, self-management
	// should start a new file when it gets more and better features
	// TOOD: constructor, shrink, expand
	// TODO: open, isOpen, close
	// template<std::size_t Size = PAGE_SIZE, std::size_t Expand = EXPAND_SIZE, std::size_t Shrink = SHRINK_SIZE>
	template<std::size_t Size = PAGE_SIZE>
	struct NaiveContainer : std::vector<char> {
		using page_type = BasicPage<NaiveContainer, Size>;
		constexpr static auto page_size = page_type::page_size;
	public:
		inline NaiveContainer(size_type size = Size) : std::vector<char>(size, 0) {
		}

		inline NaiveContainer(const NaiveContainer &other) : std::vector<char>(other) {
		}

		inline NaiveContainer(NaiveContainer &&other) : std::vector<char>(std::move(other)) {
		}
	};

	// BasicPage is mostly used in this format
	using Container = NaiveContainer<>;
	using Page = typename Container::page_type;
}
