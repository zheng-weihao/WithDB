#pragma once

#include "cache.hpp"
#include "drive.hpp"
#include "translator.hpp"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace db {
	struct keeper: cache_handler<address, page> {
		using shared_info_pair = std::pair<keeper &, cache<address, page> &>;
		using shared_info = std::shared_ptr<shared_info_pair>;

		struct virtual_page : page {
			using shared_iter_pair = std::shared_ptr<std::pair<iterator, iterator>>;
			using shared_info = keeper::shared_info;
			shared_info info;
			address addr;
			int pin_cnt;

		public:
			inline virtual_page(shared_iter_pair &iter_pair, shared_info &info, address addr) :
				page(iter_pair), info(info), addr(addr), pin_cnt(0) {
			}

			inline virtual_page(virtual_page &other) : page(other), info(other.info), addr(other.addr), pin_cnt(0) {
			}

			inline virtual_page(virtual_page &&other) :
				page(std::move(other)), info(std::move(other.info)), addr(other.addr), pin_cnt(other.pin_cnt) {
				other.addr = 0;
				other.pin_cnt = 0;
			}

			inline virtual_page(): addr(0), pin_cnt(0) {
			}

			inline virtual_page &operator=(const virtual_page &other) {
				page::operator=(other);
				info = other.info;
				addr = other.addr;
				pin_cnt = 0;
				return *this;
			}

			inline virtual_page &operator=(virtual_page &&other)  {
				page::operator=(std::move(other));
				info = std::move(other.info);
				addr = other.addr;
				pin_cnt = other.pin_cnt;
				other.pin_cnt = 0;
				return *this;
			}

			// TODO: naive way to handle page access conflicts, use reader-writer model instead
			bool is_pinned(bool myself = false) {
				return myself ? pin_cnt > 0 : info->second.is_pinned(addr);
			}

			bool pin() {
				if (pin_cnt > 0) {
					return true;
				} else if (info->second.pin(addr)) {
					++pin_cnt;
					return true;
				} else {
					return false;
				}
			}

			void unpin() {
				if (!pin_cnt) {
					return;
				}
				if (--pin_cnt == 0) {
					info->second.unpin(addr);
				}
			}

			void reactivate() {
				while (!is_active()) {
					// TODO: ugly code
					auto result = info->first.hold(addr);
					shared_pair = std::move(result.shared_pair);
					if (shared_pair && size() > static_cast<std::ptrdiff_t>(PAGE_SIZE)) {
						throw std::out_of_range("[basic_page::constructor] space too large for page operation");
					}
					info = std::move(result.info);
					pin_cnt = result.pin_cnt;
				}
			}
		};

		drive io;
		translator trans;
		std::vector<cache<address, page>> caches;
		std::vector<shared_info> infos;

		explicit keeper(const char * filename, bool trunc = false) : io(filename, trunc), trans(io) {
		}

		explicit keeper(const std::string &filename, bool trunc = false) : keeper(filename.c_str(), trunc) {
		}

		void close() {
			save();
			trans.close();
			io.close();
		}

		// TODO: schedule clean when keeper thread is free for a long time
		void clean() {

		}

		void save() {
			for (auto &cache : caches) {
				// TODO: get rid of direct access
				for (auto &pair : cache.position_map) {
					page tmp(cache.ptrs[pair.second]);
					soft_put(pair.first, tmp);
				}
			}
		}

		void soft_get(address addr, page &value) {
			try {
				auto alloc = trans(addr);
				io.get(value, alloc);
			} catch (std::runtime_error e) {
				value.clear();
			}
		}

		void soft_put(address addr, page &value) {
			if (!value.is_active()) {
				return;
			}
			drive_address alloc;
			try {
				alloc = trans(addr);
			} catch (std::runtime_error e) {
				alloc = io.allocate();
				trans.link(addr, alloc);
			}
			io.put(value, alloc);
		}

		// auto load and save
		virtual bool cache_insert(address addr, page &value) {
			soft_get(addr, value);
			return true;
		}

		virtual bool cache_erase(address addr, page &value) {
			soft_put(addr, value);
			return true;
		}

		// TODO: replace tasks with promise and argument calling list ?
		// TODO: design or use lock-free deque data structure
		std::deque<std::packaged_task<virtual_page()>> tasks;
		std::mutex tasks_mutex;
		std::condition_variable tasks_not_empty;
		bool start_flag;
		std::mutex start_flag_mutex;
		
		// TODO: segment retrieve and management

		virtual_page hold_func(address addr) {
			auto level = segment_cache_level(trans.find_seg(addr));
			auto &cache = caches[level];
			auto tmp = cache.get(addr);
			virtual_page tmp_page(tmp.shared_pair, infos[level], addr);
			tmp_page.pin();
			return std::move(tmp_page);
		}

		virtual_page loosen_func(address addr) {
			auto level = segment_cache_level(trans.find_seg(addr));
			auto &cache = caches[level];
			auto tmp = cache.get(addr);
			soft_put(addr, tmp); // TODO: have to write back a soft get page for unlink, stupid
			trans.unlink(addr);
			return virtual_page();
		}

		void thread_execute() {
			std::unique_lock<std::mutex> lock(tasks_mutex);
			int counter = 0;
			while (tasks.empty()) {
				if (counter++ == 6) {
					return; // check if finished
				}
				using namespace std::chrono_literals;
				tasks_not_empty.wait_for(lock, 500ms);
			}
			
			auto tmp = std::move(tasks.front());
			tasks.pop_front();
			lock.unlock();
			tmp();
		}

		void thread_loop() {
			while (true) {
				std::unique_lock<std::mutex> lock(start_flag_mutex);
				if (start_flag == false) {
					return;
				} else {
					lock.unlock();
				}
				thread_execute();
			}
		}

		std::thread event_loop;

		bool start() {
			std::unique_lock<std::mutex> lock(start_flag_mutex);
			if (start_flag == true) {
				return false;
			}
			start_flag = true;
			// init cache
			for (auto i = 0; i < KEEPER_CACHE_LEVEL; ++i) {
				caches.emplace_back(KEEPER_CACHE_LEVEL_SIZES[i], *this);;
			}
			for (auto i = 0; i < KEEPER_CACHE_LEVEL; ++i) {
				infos.emplace_back(std::make_shared<shared_info_pair>(*this, caches[i]));
			}
			
			event_loop = std::thread([this]() { this->thread_loop(); });
			return true;
		}

		void stop() {
			std::unique_lock<std::mutex> lock(start_flag_mutex);
			start_flag = false;
			lock.unlock();
			event_loop.join();
			save();
			// clear cache
			infos.clear();
			caches.clear();
		}

		void add_task(std::packaged_task<virtual_page()> &&task) {
			std::unique_lock<std::mutex> lock(tasks_mutex);
			tasks.push_back(std::move(task));
			tasks_not_empty.notify_all();
		}

		std::future<virtual_page> hold_async(address addr) {
			std::packaged_task<virtual_page()> tmp([this, addr]() {
				return std::move(this->hold_func(addr));
			});
			auto result = tmp.get_future();
			add_task(std::move(tmp));
			return std::move(result);
		}

		std::future<virtual_page> loosen_async(address addr) {
			std::packaged_task<virtual_page()> tmp([this, addr]() {
				return std::move(this->loosen_func(addr));
			});

			auto result = tmp.get_future();
			add_task(std::move(tmp));
			return std::move(result);
		}

		virtual_page hold(address addr) {
			auto result = hold_async(addr);
			result.wait();
			return result.get();
		}

		virtual_page loosen(address addr) {
			auto result = loosen_async(addr);
			result.wait();
			return result.get();
		}
		
		std::string get_name() {
			return trans.entry.get_database_name();
		}

		void set_name(const std::string &name) {
			trans.entry.set_database_name(name);
		}

	};

	using virtual_page = keeper::virtual_page;
}