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
		using check_element = std::enable_if<std::is_same_v<typename iterator_t<Type>::value_type, element_t>>;

		template<typename Type>
		using check_random_access = std::enable_if<std::is_base_of_v<std::random_access_iterator_tag, typename iterator_t<Type>::iterator_category>>;

		template<typename Type>
		using check_container = std::void_t<typename check_element<Type>::type, typename check_random_access<Type>::type>;
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
	template<typename Container, size_t Size = PAGE_SIZE
		, ns::page::check_container<Container> * = nullptr
	> struct BasicPage {
		using value_type = typename Container::value_type;
		using iterator = typename Container::iterator;
		// TODO: maybe other types defined by container

		Container &_container;
		size_t _begin, _end;

	public:
		inline BasicPage(Container &container, iterator first, iterator last) : _container(container) {
			activate(first, last);
		}
		
		inline BasicPage(Container &container, iterator pos) : BasicPage(container, pos, pos + Size) {
		}

		inline explicit BasicPage(Container &container, size_t first, size_t last)
			: BasicPage(container, container.begin() + first, container.begin() + last) {
		}

		inline BasicPage(Container &container, size_t pos) : BasicPage(container, container.begin() + pos) {
		}

		inline BasicPage(Container &container) : _container(container), _begin(0), _end(0) {
		}

		inline BasicPage(const BasicPage &other) : BasicPage(other._container, other._begin, other._end) {
		}

		inline BasicPage(BasicPage &&other) : BasicPage(other) {
			other.deactivate();
		}

		inline BasicPage &operator=(const BasicPage &other) {
			if (&_container != &other._container) {
				throw std::runtime_error("[BasicPage::operator=]");
			}
			activate(other.begin(), other.end());
			return *this;
		}

		inline BasicPage &operator=(BasicPage &&other) {
			if (&_container != &other._container) {
				throw std::runtime_error("[BasicPage::operator=]");
			}
			activate(other.begin(), other.end());
			other.deactivate();
			return *this;
		}

		inline size_t size() const { return _end - _begin; }

		inline iterator begin() const { return _container.begin() + _begin; }

		inline iterator end() const { return _container.begin() + _end; }

		inline value_type *data() const { return _container.data() + _begin; }

		inline bool isActive() const { return _end != 0; }

		inline void activate(iterator first, iterator last) {
			if (last < first || Size < last - first) {
				throw std::runtime_error("[BasicPage::activate]");
			}
			_begin = std::distance(_container.begin(), first);
			_end = std::distance(_container.begin(), last);
		}

		// TODO: this code style is better for debugging, not know anything about its efficiency, include code in the constructor
		inline void activate(size_t first, size_t last) { activate(_container.begin() + first, _container.begin() + last); }

		inline void deactivate() { _end = _begin = 0; }

		inline void resize(size_t size) { activate(_begin, _begin + size); }

		inline void clear() { std::fill(begin(), end(), '\0'); }

		// read-write wrapper for read-write in endian.hpp, using page_address is safer with fixed container
		// only deal with read small, so return new value directly without reference // TODO: can we add the function read with reference?
		template<typename Value>
		inline Value read(size_t first, size_t last) const  {
			if (first < last && last <= size()) {
				return db::read<Value>(begin() + first, begin() + last); // access the read funtion in endian.hpp
			} else {
				throw std::runtime_error("[page::read]");
			}
		}
		
		template<typename Value>
		inline Value read(size_t pos) const { return read<Value>(pos, size()); }

		template<typename Value>
		inline void write(const Value &value, size_t first, size_t last) {
			if (first < last && last <= size()) {
				db::write(value, begin() + first, begin() + last);
			} else {
				throw std::runtime_error("[page::write]");
			}
		}

		template<typename Value>
		inline void write(const Value &value, size_t pos) {
			write(value, pos, size());
		}

		// load-dump function for sync data base on the basic page with page memory in container
		virtual bool load() {
			return true;
		}
		// dump must be virtual when cache close and dump everything back, we can't know the type, load is virtual to keep consistent with dump
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
	template<typename Address, typename Container, size_t PageSize, typename Handler, typename Core>
	struct Cache<Address, BasicPage<Container, PageSize>, Handler, Core> : Core {
		using Super = Core;
		using Page = BasicPage<Container, PageSize>;
		using value_type = Page; // using page to avoid reference to reference in insert and erase

		Handler &_handler;
		Container _container;

		std::vector<std::shared_ptr<Page>> _ptrs; // because ptrs are full in most of the status, init and destroy in unordered_map is not appropriate
		std::unordered_map<Address, size_t> _map; // record mapping between address and ptrs index
	public:
		inline Cache(Handler &handler, size_t capacity = 0)
			: Super(capacity), _handler(handler), _container(capacity * PageSize), _ptrs(capacity) {
		}

		inline Cache(const Cache &other)
			: Super(other), _handler(other._handler), _container(other._container), _ptrs(other._ptrs), _map(other._map) {
		}

		inline Cache(Cache &&other)
			: Super(std::move(other)), _handler(other._handler), _container(std::move(other._container))
			, _ptrs(std::move(other._ptrs)), _map(std::move(other._map)) {
		}

		inline ~Cache() {
			close();
		}

		inline bool isOpen() {
			return Super::isOpen();
		}

		inline void open(size_t capacity) {
			if (isOpen()) {
				throw std::runtime_error("[Cache::open]");
			}
			Super::open(capacity);
			_container.resize(capacity * PageSize); // TODO: use naive_contianer's vector function, should be discard
			_ptrs.resize(capacity);
		}

		template<typename... Args>
		inline void close(const Args &... args) {
			if (!isOpen()) {
				return;
			}
			Super::close();
			for (auto &p : _map) {
				if (!_handler.onErase(p.first, *_ptrs[p.second], args...)) {
					throw std::runtime_error("[Cache::close]");
				}
			}
			_map.clear();
			_ptrs.clear();
			_container.clear();
		}

		inline Container &container() { return _container; }

		inline size_t capacity() { return _ptrs.size(); }

		inline size_t size() { return _map.size(); }

		// these hit-insert-erase is not applied on ptrs
		// because I don't want them to know about the fecth return type information to invoke more template
		// take index to insert on choosen position
		// return == _ptrs.size() means failure
		template<typename... Args>
		inline size_t hit(Address addr, const Args &... args) {
			auto iter = _map.find(addr);
			if (iter == _map.end()) {
				return _ptrs.size();
			}
			auto index = iter->second;
			return _handler.onHit(addr, *_ptrs[index], args...) ? index : _ptrs.size();
		}

		template<typename... Args>
		inline bool insert(Address addr, size_t index, const Args &... args) {
			auto &ptr = _ptrs[index];
			return ptr && _handler.onInsert(addr, *ptr, args...) && _map.try_emplace(addr, index).second;
		}

		// return index for the addr, else return size of ptrs if fail
		template<typename... Args>
		inline size_t erase(Address addr, const Args &... args) {
			auto iter = _map.find(addr);
			if (iter == _map.end()) {
				return _ptrs.size();
			}

			auto index = iter->second;
			_map.erase(iter);
			auto &ptr = _ptrs[index];
			
			if (_handler.onErase(addr, *ptr, args...)) {
				ptr->deactivate();
				ptr.reset();
				return index;
			} else {
				return _ptrs.size();
			}
		}

		// DESIGN:
		// if ptr stores nullptr, copy shared_ptr and assign to ptr when hit
		// create new ptr and store, assign it to ptr when miss
		// if ptr stores page, move data from ptr to the hit ptr and replace ptr's pointer with the hit ptr when hit
		// store giving ptr and call onInsert on it when miss, for save receive time
		// partial load is handled in handler.onInsert, flag is passed by args
		template<typename Derived
			, ns::page::check_base_t<Page, Derived> * = nullptr
			, typename... Args
		> inline bool collect(Address addr, std::shared_ptr<Derived> &ptr, const Args &... args) {
			auto index = hit(addr, args...);
			// hit
			if (index != _ptrs.size()) {
				if (Super::hit(addr)) { // log hit in core for algorithm and other usage
					// TODO: I think static cast is enough in this case (mostly Cache only contains the same type of derived page), and dynamic cast might limit our ability, need a better consideration
					auto tmp = std::static_pointer_cast<Derived>(_ptrs[index]);
					if (ptr) {
						throw std::runtime_error("[Cache::collect]page hit conflict");
						*tmp = std::move(*ptr); // TODO: conflict, throw exception
					}
					ptr = std::move(tmp);
					return true;
				} else {
					// cannot access here: if structure hit but core should hit
					throw std::runtime_error("[Cache::collect]");
				}
			}

			// miss, find insert place
			auto result = Super::replace(addr);
			if (result == addr) {
				for (index = 0; index != _ptrs.size(); ++index) {
					if (!_ptrs[index]) {
						break;
					}
				}
			} else {
				index = erase(result, args...);
			}
			if (index == _ptrs.size()) {
				return false;
			}

			if (!ptr) {
				ptr = std::make_shared<Derived>(_handler.onCreate<Derived>(args...));
			}

			// &(ptr->_container) != &_container, skip this checking, I am confident about it
			(_ptrs[index] = std::static_pointer_cast<Page>(ptr))->activate(index * PageSize, index * PageSize + PageSize);
			if (insert(addr, index, args...) && Super::insert(addr)) { // add to replace when success
				return true;
			} else {
				_ptrs[index].reset();
				ptr.reset();
				return false;
			}
		}

		template<typename Derived
			, ns::page::check_base_t<Page, Derived> * = nullptr
			, typename... Args
		> inline std::shared_ptr<Derived> fetch(Address addr, const Args &... args) {
			std::shared_ptr<Derived> ptr;
			if (collect(addr, ptr, args...)) {
				return ptr;
			}
			throw std::runtime_error("[Cache::fetch]");
		}

		template<typename... Args>
		inline bool discard(Address addr, const Args &... args) {
			return Super::erase(addr) && erase(addr, args...) != _ptrs.size();
		}
	};

	// DESIGN:
	// page actual owner in memory, need to be optimized for higher performance and more feature

	// TODO: a lot to do with container, mmap(slow start up), flexible memory, self-management
	// should start a new file when it gets more and better features
	// TOOD: constructor, shrink, expand
	// TODO: open, isOpen, close
	// template<std::size_t Size = PAGE_SIZE, std::size_t Expand = EXPAND_SIZE, std::size_t Shrink = SHRINK_SIZE>
	template<size_t Size = PAGE_SIZE>
	struct NaiveContainer : std::vector<element_t> {
		using Page = BasicPage<NaiveContainer, Size>;

	public:
		inline NaiveContainer(size_t size = Size) : std::vector<char>(size) {
		}

		inline NaiveContainer(const NaiveContainer &other) : std::vector<char>(other) {
		}

		inline NaiveContainer(NaiveContainer &&other) : std::vector<char>(std::move(other)) {
		}
	};

	// BasicPage is mostly used in this format
	using Container = NaiveContainer<>;
	using Page = typename Container::Page;

	inline std::vector<element_t> debug(const Page &page) { return std::vector<element_t>(page.begin(), page.end()); }
}
