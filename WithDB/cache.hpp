#ifndef __CACHE_HPP__
#define __CACHE_HPP__

#include "page.hpp"
#include "definitions.hpp"

#include <algorithm>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

// replace: find item to kick, kick item, load new comer(if error recover to the time after kicking)
// never know what will cause exception when handle insert
// TODO: find lock-free structure or elegant way to lock
// TOOD: only pin-unpin need lock, event_loop keep replace operation, is that true?
// access is meanless, thread should pin their page when before access page
// TODO: reader-writer problem in pin page
namespace db {
	template<typename Address>
	struct cache_replace {
		struct cache_log {
			int pin_cnt;
			timestamp accessAt;
			cache_log(timestamp accessAt) : pin_cnt(0), accessAt(accessAt) {
			}
		};
		std::unordered_map<Address, cache_log> logs;
		std::size_t limit;
		std::mutex access_mutex;
	public:
		cache_replace(std::size_t limit): limit(limit) {
		}

		cache_replace(const cache_replace &other): logs(logs), limit(limit) {
		}

		cache_replace(cache_replace &&other) :
			logs(std::move(logs)), limit(limit) {
			
		}

		// replace with MRU
		Address operator()(Address addr) {
			std::unique_lock<std::mutex> lock(access_mutex);
			auto ret = addr;
			if (logs.size() >= limit) {
				using comp_type = decltype(*(logs.begin()));
				auto comp = [](const comp_type &a, const comp_type &b) -> bool {
					if ((a.second.pin_cnt > 0) ^ (b.second.pin_cnt > 0)) {
						return b.second.pin_cnt > 0;
					} else {
						return a.second.accessAt > b.second.accessAt;
					}
				};
				auto iter = std::min_element(logs.begin(), logs.end(), comp);
				if (iter->second.pin_cnt > 0) {
					throw std::runtime_error("[cache_replace] all addresses are pinned");
				} else {
					ret = iter->first;
					logs.erase(iter);
				}
			}
			return ret;
		}

		void success(Address addr) {
			std::unique_lock<std::mutex> lock(access_mutex);
			logs.insert(std::make_pair(addr, cache_log(current_timestamp())));
		}

		void access(Address addr) {
			std::unique_lock<std::mutex> lock(access_mutex);
			auto iter = logs.find(addr);
			if (iter == logs.end()) {
				throw std::runtime_error("[cache_replace::access] cannot find address in logs");
			} else {
				iter->second.accessAt = current_timestamp();
			}
		}

		bool is_pinned(Address addr) {
			std::unique_lock<std::mutex> lock(access_mutex);
			auto iter = logs.find(addr);
			return iter != logs.end() && iter->second.pin_cnt > 0;
		}

		bool pin(Address addr) {
			std::unique_lock<std::mutex> lock(access_mutex);
			auto iter = logs.find(addr);
			if (iter == logs.end()) {
				throw std::runtime_error("[cache_replace::pin] cannot find address in logs");
			} else {
				if (iter->second.pin_cnt) {
					return false;
				} else {
					iter->second.pin_cnt += 1;
					return true;
				}
				
			}
		}

		void unpin(Address addr) {
			std::unique_lock<std::mutex> lock(access_mutex);
			auto iter = logs.find(addr);
			if (iter == logs.end()) {
				// throw std::runtime_error("[cache_replace::unpin] cannot find address in logs");
			} else {
				if (!iter->second.pin_cnt) {
					return;
				}
				iter->second.pin_cnt -= 1;
				if (iter->second.pin_cnt <= 0) {
					iter->second.accessAt = current_timestamp();
				}
			}
		}
	};

	// interface handler for cache insert-erase operation
	// handler can change its own status for the operation
	template<typename Address, typename Type>
	struct cache_handler {
		// handle cache insert and put insert mapping value in &value
		virtual bool cache_insert(Address addr, Type &value) = 0;
		// handle cache erase and put mapping value in &value for cleaning/write_back ...
		virtual bool cache_erase(Address addr, Type &value) = 0;
	};


	// TODO: maybe consider flexible cache memory in the future
	// fix size cache
	template<typename Address, typename Type>
	struct cache {
		std::unordered_map<Address, Type> value_map;
		cache_replace<Address> replace;
		cache_handler<Address, Type> &handler;
	public:
		cache(std::size_t size, cache_handler<Address, Type> &handler) : replace(size), handler(handler) {
		}

		cache(const cache &other) : cache(other.ptrs.size(), other.handler) {
		}

		cache(cache &&other) :
			value_map(std::move(other.value_map)),
			replace(std::move(other.replace)), handler(other.handler) {

		}

		bool insert(Address addr, Type &value) {
			auto flag = handler.cache_insert(addr, value);
			if (flag) {
				value_map.insert(std::make_pair(addr, value));
				replace.success(addr); // add to replace when success
			}
			return flag;
		}

		bool erase(Address addr) {
			auto value = value_map[addr];
			auto flag = value_map.erase(addr);
			handler.cache_erase(addr, value);
			return flag;
		}

		Type get(Address addr) {
			auto iter = value_map.find(addr);
			if (iter != value_map.end()) {
				replace.access(addr);
			} else {
				auto result = replace(addr);
				if (result != addr) {
					erase(result);
				}
				Type tmp;
				bool flag = insert(addr, tmp);
				if (!flag) {
					throw std::runtime_error("[cache::get] cannot find address mapping value");
				}
				iter = value_map.find(addr);
			}
			return iter->second;
		}

		bool is_pinned(Address addr) {
			return replace.is_pinned(addr);
		}

		bool pin(Address addr) {
			replace.pin(addr);
		}

		void unpin(Address addr) {
			replace.unpin(addr);
		}
	};

	template<typename Address>
	struct cache<Address, page> {
		using control_pair = std::pair<typename std::vector<char>::iterator, typename std::vector<char>::iterator>;
		using control_ptr = std::shared_ptr<control_pair>;
		std::vector<char> memory;
		std::vector<control_ptr> ptrs;
		std::unordered_map<Address, std::size_t> position_map;
		cache_replace<Address> replace;
		cache_handler<Address, page> &handler;
	public:
		cache(std::size_t size, cache_handler<Address, page> &handler) : memory(size * PAGE_SIZE),
			ptrs(size), replace(size), handler(handler) {
		}

		cache(const cache &other): cache(other.ptrs.size(), other.handler){
		}

		cache(cache &&other) :
			memory(std::move(other.memory)),
			ptrs(std::move(other.ptrs)),
			position_map(std::move(other.position_map)),
			replace(std::move(other.replace)), handler(other.handler) {

		}

		bool insert(Address addr, page &value) {
			bool flag = handler.cache_insert(addr, value);
			// TODO: hacking way to get page index in cache
			if (flag) {
				position_map.insert(std::make_pair(addr, (value.begin() - memory.begin()) / PAGE_SIZE));
				replace.success(addr);
			}
			return flag;
		}

		bool erase(Address addr) {
			auto index = position_map[addr];
			position_map.erase(addr);
			page tmp(std::move(ptrs[index]));
			auto flag = handler.cache_erase(addr, tmp);
			tmp.deactivate();
			return flag;
		}

		page get(Address addr) {
			auto iter = position_map.find(addr);
			std::size_t index = 0;
			if (iter != position_map.end()) {
				index = iter->second;
				replace.access(addr);
			} else {
				auto result = replace(addr);
				if (result != addr) {
					index = position_map[result];
					erase(result);
				} else {
					for (index = 0; index < ptrs.size(); ++index) {
						if (!ptrs[index]) {
							break;
						}
					}
				}
				ptrs[index].reset();
				auto alloc_begin = memory.begin() + index * PAGE_SIZE;
				auto alloc_end = alloc_begin + PAGE_SIZE;
				auto tmp = std::make_shared<control_pair>(std::make_pair(alloc_begin, alloc_end));
				page tmp_page(tmp);
				bool flag = insert(addr, tmp_page);
				if (!flag) {
					throw std::runtime_error("[cache::get] cannot find address mapping value");
				}
				ptrs[index] = std::move(tmp);
			}
			return page(ptrs[index]);
		}

		bool is_pinned(Address addr) {
			return replace.is_pinned(addr);
		}

		bool pin(Address addr) {
			return replace.pin(addr);
		}

		void unpin(Address addr) {
			replace.unpin(addr);
		}
	};
}

#endif // __CACHE_HPP__
