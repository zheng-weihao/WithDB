#pragma once

#include "definitions.hpp"
#include "utils.hpp"

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <functional>
#include <vector>

// TODO: I think a better structure is a hash table with vector for metrics storing
// TODO: mru might need a better design, like while search, the threshold enlarged for a fast but not so accurate replacement, but lower io exchange is our main target for sure
// TODO: shrink and expand Cache handling

// DESIGN:
// Cache is not thread-safe
// all those thread safety should be managed in higher level, like all guard, at least virtual page level

namespace db {
	// DESIGN:
	// CacheCore is a set of algorithm with a dynamic capacity for Cache exchange
	// replace without flag reference will throw an exception when find no item to kick, caller should keep notice this
	// replace operator(): find item to kick if full, and kick item (even we know that Cache load might fail)
	// Cache should put new address manually (if error recover to the time after kicking)
	// because we never know what will cause exception when handle Cache insert
	// calling operator() won't put the new value in its store, but will kick one if full (if cache_handler process with an error, we will have a wrong one keeping in the store, and cause exception when kick it)
	// after new address successfully load, Cache should put the new address in the replacement list with pinned status
	// CacheCore pin status is only used for Cache exchange choosing and should not be used as a flag for multi-thread to check availability
	// capacity == 0 represent closed status
	//  = [](Address addr) {return static_cast<size_t>(addr) / PAGE_SIZE; }
	// TODO: maybe core should keep index information for stored address
	template<typename Address>
	using AddressHash = size_t(*)(Address);
	template<typename Address, AddressHash<Address> Hash> // TODO: best practice to pass constexpr lambda
	struct HashCacheCore {
		std::vector<bool> _flags;
		std::vector<Address> _addresses;
	public:
		inline HashCacheCore(size_t capacity = 0) : _flags(capacity), _addresses(capacity) {
		}

		inline HashCacheCore(const HashCacheCore &other) : _flags(other._flags), _addresses(other._addresses) {
		}

		inline HashCacheCore(HashCacheCore &&other) :_flags(std::move(other._flags)), _addresses(std::move(other._addresses)) {
		}

		inline ~HashCacheCore() {
			close();
		}

		inline bool isOpen() {
			return _flags.size() != 0;
		}

		inline void open(std::size_t capacity) {
			if (isOpen()) {
				throw std::runtime_error("[MRUCacheCore::open]");
			}
			_flags.resize(capacity);
			_addresses.resize(capacity);
		}

		inline void close() {
			if (!isOpen()) {
				return;
			}
			_flags.clear();
			_addresses.clear();
		}

		inline size_t capacity() {
			return _flags.size();
		}

		inline size_t hash(Address addr) {
			return Hash(addr) % capacity();
		}

		// TODO: might have other changable arguements for Metrics
		inline bool hit(Address addr) { // TODO: maybe change to hit and return index for address
			return true; // just pass, handle event when hit
		}

		inline bool insert(Address addr) {
			auto code = hash(addr);
			if (_flags[code]) {
				return false;
			}
			_flags[code] = true;
			_addresses[code] = addr;
			return true;
		}

		inline bool erase(Address addr) {
			auto code = hash(addr);
			if (_flags[code] && _addresses[code] == addr) {
				_flags[code] = false;
				return true;
			}
			return false;
		}

		// addr is for further use in choicing kick item algorithm
		inline Address replace(Address addr, bool &flag) {
			flag = true;
			auto code = hash(addr);
			auto ret = _flags[code] ? _addresses[code] : addr;
			_flags[code] = false;
			return ret;
		}

		// throw a exception without setting the flag
		inline Address replace(Address addr) {
			bool flag;
			return replace(addr, flag);
		}
	};

	template<typename Address>
	struct MRUCacheCore {
		struct Metrics {
			bool _pin; // change to control bitset if there are other flags to save
			timestamp _accessAt;
			inline Metrics(bool pin, timestamp accessAt) : _pin(pin), _accessAt(accessAt) {
			}
			inline Metrics(bool pin = false) : Metrics(pin, current()) {
			}
		};
		
		// capacity can be flexible, so it's not a template parameter
		size_t _capacity; // TODO: change to capacity
		std::unordered_map<Address, Metrics> _items;

	public:
		inline MRUCacheCore(size_t capacity = 0) : _capacity(capacity) {
		}

		inline MRUCacheCore(const MRUCacheCore &other) : _capacity(other._capacity), _items(other._items) {
		}

		inline MRUCacheCore(MRUCacheCore &&other) : _capacity(other._capacity), _items(std::move(other._items)) {
			other._capacity = 0;
		}

		inline ~MRUCacheCore() {
			close();
		}

		inline bool isOpen() {
			return _capacity != 0;
		}

		inline void open(std::size_t capacity) {
			if (isOpen()) {
				throw std::runtime_error("[MRUCacheCore::open]");
			}
			_capacity = capacity;
		}

		inline void close() {
			if (!isOpen()) {
				return;
			}
			for (auto &p : _items) {
				if (p.second._pin) {
					throw std::runtime_error("[MRUCacheCore::close]"); // close with pinned address should not be allowd
				}
			}
			_items.clear();
			_capacity = 0;
		}

		inline size_t size() {
			return _items.size();
		}

		inline size_t capacity() {
			return _capacity;
		}

		inline bool isFull() {
			return size() == _capacity;
		}

		// TODO: might have other changable arguements for Metrics
		inline bool hit(Address addr) {
			auto iter = _items.find(addr);
			if (iter == _items.end()) {
				return false;
			}
			iter->second._accessAt = current();
			return true;
		}

		inline bool insert(Address addr, bool pin = false) {
			if (isFull()) {
				return false;
			}
			_items.insert(std::make_pair(addr, Metrics(pin)));
			return true;
		}

		inline bool erase(Address addr) {
			auto iter = _items.find(addr);
			if (iter == _items.end() || iter->second._pin) {
				return false;
			}
			_items.erase(iter);
			return true;
		}

		// replace with MRU
		// addr is for further use in choicing kick item algorithm
		inline Address replace(Address addr, bool &flag) {
			flag = true;
			if (!isFull()) {
				return addr;
			}
			using compare_t = typename decltype(_items)::value_type;
			auto iter = std::min_element(_items.begin(), _items.end(),
				[](const compare_t &a, const compare_t &b) {
				if (a.second._pin ^ b.second._pin) {
					return b.second._pin;
				} else {
					return a.second._accessAt > b.second._accessAt;
				}}
			);
			if (iter->second._pin) {
				flag = false;
				return addr;
			} else {
				auto ret = iter->first;
				_items.erase(iter);
				return ret;
			}
		}
		
		// throw a exception without setting the flag
		inline Address replace(Address addr) {
			bool flag = false;
			addr = replace(addr, flag);
			if (!flag) {
				throw std::runtime_error("[MRUCacheCore::operator()]");
			}
			return addr;
		}

		inline bool isPinned(Address addr) {
			auto iter = _items.find(addr);
			if (iter == _items.end()) {
				throw std::runtime_error("[MRUCacheCore::pin]");
			}
			return iter->second._pin;
		}

		inline bool pin(Address addr) {
			auto iter = _items.find(addr);
			if (iter == _items.end()) {
				return false;
				// throw std::runtime_error("[MRUCacheCore::pin]");
			}
			iter->second._pin = true;
			// timestamp don't need to change because pinned items can't be kicked
			return true;
		}

		inline bool unpin(Address addr) {
			auto iter = _items.find(addr);
			if (iter == _items.end()) {
				return false;
				// throw std::runtime_error("[MRUCacheCore::unpin]");
			}
			iter->second._pin = false;
			iter->second._accessAt = current();  // record unpin timestamp
			return true;
		}
	};

	// universal interface to handle Cache insert-erase-hit event
	// handler can change its own status for these operation
	template<typename Address, typename Value>
	struct BasicCacheHandler {
		// handle Cache insert and put insert mapping value in &value
		inline bool onInsert(Address addr, Value &value) {
			return true;
		}
		// default Cache will not trigger hit event, it's used for handling partial load problem
		inline bool onHit(Address addr, Value &value) {
			return true;
		}
		// handle Cache erase and put mapping value in &value for cleaning/write_back ...
		inline bool onErase(Address addr, Value &value) {
			return true;
		}
	};

	// DESIGN:
	// Cache is a key-value structure in my opinion, so the core feature is to access value freely without caring about the insert-erase staff
	// Cache handler is designed for higher levels to control the insert-erase behavior, because only these parts know how to do it
	// Cache is at the lower level, because it can be reused a lot in many parts
	// Cache address type and value type shoud be simple enough for value copy, and has default constructor
	// replace closed represent closed status
	// commonly used in tlb
	// TODO: maybe consider flexible Cache memory in the future, shrink-expand
	template<typename Address, typename Value, typename Handler, typename Core = MRUCacheCore<Address>>
	struct Cache: Core {
		using super = Core;
		using address_type = Address;
		using value_type = Value;
		using handler_type = Handler;
		using core_type = Core;
		
		Handler &_handler;
		std::unordered_map<Address, Value> _values;
		
	public:
		inline Cache(Handler &handler, size_t capacity = 0) : super(capacity), _handler(handler) {
		}

		inline Cache(const Cache &other) : super(other), _handler(other._handler), _values(other._values) {
		}

		inline Cache(Cache &&other) : super(std::move(other)), _handler(other._handler), _values(std::move(other._values)) {
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
		}

		inline void close() {
			if (!super::isOpen()) {
				return;
			}
			super::close();
			for (auto &v : _values) {
				bool result = _handler.onErase(v.first, v.second);
				if (!result) {
					throw std::runtime_error("[Cache::close]");
				}
			}
			_values.clear();
		}

		inline bool hit(Address addr, Value &value) {
			return super::hit(addr) && _handler.onHit(addr, value); // TODO: more feature
		}

		inline bool insert(Address addr, Value &value) {
			auto flag = _handler.onInsert(addr, value);
			if (flag) {
				flag = _values.insert(std::make_pair(addr, value)).second;
			}
			return flag;
		}

		// with no exception, private function, not in core
		inline bool erase(Address addr) {
			auto iter = _values.find(addr);
			if (iter == _values.end()) {
				return false;
			}
			bool flag = _handler.onErase(addr, iter->second);
			_values.erase(iter);
			return flag;
		}

		inline Value fetch(Address addr, bool &flag) { // TODO: default argument list on fetch function design to init default value
			flag = true;
			auto iter = _values.find(addr);
			if (iter != _values.end()) {
				flag = hit(addr, iter->second);
				if (flag) {
					return iter->second;
				} else {
					return defaultValue<Value>(); // TODO: use arguement list to remove
				}
			}
			auto res = super::replace(addr);
			if (res != addr) {
				flag = erase(res);
				if (!flag) {
					return defaultValue<Value>(); // TODO: use arguement list to remove
				}
			}
			Value tmp;
			flag = insert(addr, tmp);
			if (flag) {
				flag = super::insert(addr); // add to replace when success
			}
			return tmp;
		}

		inline Value fetch(Address addr) { // TODO: same problem
			bool flag;
			Value tmp = fetch(addr, flag);
			if (!flag) {
				throw std::runtime_error("[Cache::fetch]");
			}
			return tmp;
		}

		inline bool discard(Address addr) { // force kick out
			return super::erase(addr) && erase(addr);
		}
	};
}
