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
	namespace ns::keeper {
		template<typename Base, typename Derived>
		using check_base = std::enable_if<std::is_base_of_v<Base, Derived>>;
		template<typename Page, typename Derived>
		using check_base_t = typename check_base<Page, Derived>::type;
	}

	struct Keeper: BasicCacheHandler<address, Page> {
		using cache_type = Cache<address, Page, Keeper>;
		using container_type = typename cache_type;

		constexpr static size_t getLevel(address addr) {
			auto index = addr >> SEGMENT_BIT_LENGTH;
			if (index < Translator::segmentEnd(METADATA_SEG)) {
				return 0;
			}
			if (index < Translator::segmentEnd(BLOB_SEG)) {
				return 1;
			}
			if (index < Translator::segmentEnd(DATA_SEG)) {
				return 2;
			}
			if (index < Translator::segmentEnd(INDEX_SEG)) {
				return 1;
			}
			if (index < Translator::segmentEnd(TEMP_SEG)) {
				return 2;
			}
			return -1;
		}

		struct VirtualPage : Page {
			Keeper &_keeper;
			address _addr;
			address _length;

		public:
			inline VirtualPage(container_type &container, Keeper &keeper, address addr, address length = PAGE_SIZE) : Page(keeper._caches[getLevel(addr)]._container), _keeper(keeper), _addr(addr), _length(length) {
			}

			inline VirtualPage(container_type &container, size_t first, size_t last, Keeper &keeper, address addr, address length = PAGE_SIZE) : Page(container, first, last), _keeper(keeper), _addr(addr), _length(length) {
			}

			inline VirtualPage(container_type &container, size_t first, size_t last, const VirtualPage &page) : Page(container, first, last), _keeper(page._keeper), _addr(page._addr), _length(page._length) {
			}

			inline VirtualPage(container_type &container, size_t first, size_t last, VirtualPage &&page) : Page(container, first, last), _keeper(page._keeper), _addr(page._addr), _length(page._length) {
			}

			inline VirtualPage(VirtualPage &other) : Page(other), _keeper(other._keeper), _addr(other._addr), _length(other._addr) {
			}

			inline VirtualPage(VirtualPage &&other) :
				Page(std::move(other)), _keeper(other._keeper), _addr(other._addr), _length(other._addr) {
				other._addr = 0;
				other._length = 0;
			}

			inline VirtualPage &operator=(const VirtualPage &other) {
				Page::operator=(other);
				_addr = other._addr;
				_length = other._length;
				return *this;
			}

			inline VirtualPage &operator=(VirtualPage &&other)  {
				Page::operator=(std::move(other));
				_addr = other._addr;
				_length = other._length;
				other._addr = 0;
				other._length = 0;
				return *this;
			}

			inline Keeper::cache_type &cache() {
				return _keeper._caches[getLevel(_addr)];
			}

			// TODO: lock and unlock wrapper for reactivate and pin
			// TODO: naive way to handle page access conflicts, use reader-writer model instead
			inline bool isPinned() {
				return cache().isPinned(_addr);
			}

			inline bool pin() {
				return cache().pin(_addr);
			}

			inline bool unpin() {
				return cache().unpin(_addr);
			}

			inline void reactivate(bool pin = true) {
				while (!isActive()) {
					operator=(*_keeper.hold<VirtualPage>(_addr, pin, false));
				}
			}
		};

		Drive _drive;
		Translator _translator;
		std::vector<cache_type> _caches;

		inline explicit Keeper() : _drive(), _translator(_drive) {
		}

		inline explicit Keeper(const char * path, bool truncate = false) : Keeper() {
			open(path, truncate);
		}

		inline explicit Keeper(const std::string &path, bool truncate = false) : Keeper(path.c_str(), truncate) {
		}

		inline ~Keeper() {
			close();
		}

		inline bool isOpen() {
			return _drive.isOpen();
		}

		inline void open(const char * path, bool truncate = false) {
			if (isOpen()) {
				throw std::runtime_error("[Keeper::open]");
			}
			_drive.open(path, truncate);
			_translator.open();
		}

		inline void close() {
			if (!isOpen()) {
				return;
			}
			if (isStarted()) {
				stop();
			}
			dump();
			_translator.close();
			_drive.close();
		}

		inline void dump() {
			for (auto &cache : _caches) {
				// TODO: get rid of direct access
				for (auto &ptr : cache._ptrs) {
					if (ptr) {
						auto p = std::static_pointer_cast<VirtualPage>(ptr);
						putSoft(p->_addr, *p);
					}
				}
			}
		}

		inline std::string &name() {
			return _translator._name;
		}

		inline bool getSoft(address addr, Page &value) {
			bool flag = false;
			auto result = _translator(addr, flag);
			if (flag) {
				return _drive.get(value, result, false);
			} else {
				value.clear();
				return true;
			}
		}

		inline bool putSoft(address addr, Page &value) {
			if (!value.isActive()) {
				return false;
			}
			bool flag = false;
			auto result = _translator(addr, flag);
			if (!flag) {
				result = _drive.allocate(addr, flag);
				flag = _translator.link(addr, result);
			}
			return flag && _drive.put(value, result, false);
		}

		// dont auto load and save
		inline bool onInsert(address addr, Page &value) {
			return getSoft(addr, value);
		}

		inline bool onHit(address addr, Page &value) {
			return true;
		}

		inline bool onErase(address addr, Page &value) {
			return putSoft(addr, value);
		}

		template<typename Type
			, ns::keeper::check_base_t<VirtualPage, Type> * = nullptr
		> inline std::shared_ptr<VirtualPage> holdSync(address addr, bool pin, bool load) {
			auto &cache = _caches[getLevel(addr)];
			bool flag = false;
			auto miss = cache.hit(addr) == cache._ptrs.size();
			auto p = cache.fetch<Type>(addr, PAGE_SIZE, flag, *this, addr, PAGE_SIZE);
			if (!flag) {
				throw std::runtime_error("[Keeper::holdSync]");
			}
			if (miss && load) {
				p->load();
			}
			if (pin) {
				p->pin();
			}
			return std::static_pointer_cast<VirtualPage>(p);
		}
		
		std::shared_ptr<VirtualPage> loosenSync(address addr) {
			auto &cache = _caches[getLevel(addr)];
			auto flag = cache.discard(addr);
			_translator.unlink(addr);
			return nullptr; // return value for _tasks structure convenience
		}

		// TODO: replace tasks with promise and argument calling list ?
		// TODO: design or use lock-free deque data structure
		std::deque<std::packaged_task<std::shared_ptr<VirtualPage>()>> _tasks;
		std::mutex _tasksMutex;
		std::condition_variable _tasksNotEmpty;
		bool _start;
		std::mutex _startMutex;
		std::thread _eventThread;

		inline void threadTerm() {
			std::unique_lock<std::mutex> lock(_tasksMutex);
			if (_tasks.empty()) {
				using namespace std::chrono_literals;
				_tasksNotEmpty.wait_for(lock, 1000ms);
			}
			if (!_tasks.empty()) {
				auto task = std::move(_tasks.front());
				_tasks.pop_front();
				lock.unlock();
				task();
			}
		}

		inline void threadLoop() {
			std::unique_lock<std::mutex> lock(_startMutex, std::defer_lock);
			while (true) {
				lock.lock();
				if (_start == false) {
					break;
				}
				lock.unlock();
				threadTerm();
			}
			std::unique_lock<std::mutex> taskLock(_tasksMutex);
			while (!_tasks.empty()) {
				auto task = std::move(_tasks.front());
				_tasks.pop_front();
				task();
			}
		}

		inline bool isStarted() {
			return _start;
		}

		inline bool start() {
			std::unique_lock<std::mutex> lock(_startMutex);
			if (_start == true) {
				return false;
			}
			_start = true;
			lock.unlock();
			// init Cache
			for (auto i = 0; i < KEEPER_CACHE_LEVEL; ++i) {
				_caches.emplace_back(*this, KEEPER_CACHE_SIZES[i]);;
			}

			_eventThread = std::thread([this]() { this->threadLoop(); });
			return true;
		}

		inline void stop() {
			std::unique_lock<std::mutex> lock(_startMutex);
			_start = false;
			lock.unlock();
			_eventThread.join();
			dump();
			_caches.clear();
		}

		inline void addTask(std::packaged_task<std::shared_ptr<VirtualPage>()> &&task) {
			std::unique_lock<std::mutex> lock(_tasksMutex);
			_tasks.push_back(std::move(task));
			_tasksNotEmpty.notify_all();
		}

		template<typename Type
			, ns::keeper::check_base_t<VirtualPage, Type> * = nullptr
		> inline std::future<std::shared_ptr<VirtualPage>> holdAsync(address addr, bool pin, bool load) {
			std::packaged_task<std::shared_ptr<VirtualPage>()> task([this, pin, addr, load]() {
				return std::move(this->holdSync<Type>(addr, pin, load));
			});

			auto result = task.get_future();
			addTask(std::move(task));
			return std::move(result);
		}

		inline std::future<std::shared_ptr<VirtualPage>> loosenAsync(address addr) {
			std::packaged_task<std::shared_ptr<VirtualPage>()> task([this, addr]() {
				return std::move(this->loosenSync(addr));
			});

			auto result = task.get_future();
			addTask(std::move(task));
			return std::move(result);
		}

		template<typename Type
			, ns::keeper::check_base_t<VirtualPage, Type> * = nullptr
		> inline std::shared_ptr<Type> hold(address addr, bool pin = true, bool load = true) {
			auto result = holdAsync<Type>(addr, pin, load);
			result.wait();
			return std::static_pointer_cast<Type>(result.get());
		}

		inline void loosen(address addr) {
			auto result = loosenAsync(addr);
			result.wait();
		}

		// TODO: schedule clean when keeper thread is free for a long time
		inline void clean() {
		}
	};

	using VirtualPage = Keeper::VirtualPage;
}