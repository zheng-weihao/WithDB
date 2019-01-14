#pragma once

#include "cache.hpp"
#include "drive.hpp"
#include "translator.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <future>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

namespace db {
	constexpr static size_t getCacheLevel(address addr) {
		return db::getCacheLevel(getSegmentEnum(addr));
	}

	namespace ns::keeper {
		template<typename Base, typename Derived>
		using check_base = std::enable_if<std::is_base_of_v<Base, Derived>>;
		template<typename Page, typename Derived>
		using check_base_t = typename check_base<Page, Derived>::type;
	}

	// DESIGN: auto load means caller will call load or initialize in by itself after get page from drive or create,
	// and these procedure must follow the fetch step controlled by caller
	// concurrency and sync is ensured by the caller itself, so the keeper won't load in hit even in auto-load mode
	// (the data on page is guaranteed as loaded by last caller that miss it and insert it in cache)
	// dump is used to dump data on the page to storage format in cache memory
	// so auto-dump is safer but slower because every time the memory switch out, data will transform and dump
	// onHit will prefer to safer mode when conflict
	// dirty is the flag claiming whether is modified, and it's default set to true
	// clean page will not dump
	// pin is the flag claiming page should be pinned in cache, pin ensures that the page will not switch out, but cache can't use the space
	// onHit will prefer to pin when conflict
	// partial page is regarded as read only, so in onHit will call onInsert if we have to reload (size is not long enough)
	// and partial page will never auto-dump to affect the memory page also all the change on page will be discard
	// assumption: one page will not be fetched more than once at a timepoint, ensure by upper object
	struct Keeper: BasicCacheHandler<address, Page> {
		using Cache = db::Cache<address, Page, Keeper>;
		// TODO: temp pin flag for PagePtr pin and sync promblem in temporary and long-term pin request
		// bool flag
		constexpr static size_t PIN_POS = sizeof(size_t) * 8 - 1; // fetch with pin operation
		constexpr static size_t AUTOLOAD_POS = PIN_POS - 1; // auto load right after get, don't auto-load first when conflict
		constexpr static size_t AUTODUMP_POS = AUTOLOAD_POS - 1; // auto dump before put to drive, don't auto-dump first
		constexpr static size_t DIRTY_POS = AUTODUMP_POS - 1; // claim clean (readonly), will not call onErase handler
		constexpr static size_t TEMP_PIN_POS = DIRTY_POS - 1;

		// integer flag
		constexpr static size_t SIZE_BEGIN = 0;
		constexpr static size_t SIZE_END = SIZE_BEGIN + PAGE_BIT_LENGTH + 1; // 0x1fff
		constexpr static size_t SIZE_MASK = getMask(SIZE_BEGIN, SIZE_END);

		constexpr static size_t LEVEL_BEGIN = SIZE_END; // cache level
		constexpr static size_t LEVEL_END = LEVEL_BEGIN + 2; // 0, 1, 2, 3
		constexpr static size_t LEVEL_MASK = (1ull << LEVEL_END) - (1ull << LEVEL_BEGIN);

		constexpr static bool DEFAULT_PIN = false;
		constexpr static bool DEFAULT_AUTOLOAD = false; // DEBUG
		constexpr static bool DEFAULT_AUTODUMP = true; // DEBUG
		constexpr static bool DEFAULT_DIRTY = true; // DEBUG

		constexpr static size_t DEFAULT_FLAGS = setFlag(0ull, DEFAULT_PIN, PIN_POS)
			| setFlag(0ull, DEFAULT_AUTOLOAD, AUTOLOAD_POS)
			| setFlag(0ull, DEFAULT_AUTODUMP, AUTODUMP_POS)
			| setFlag(0ull, DEFAULT_DIRTY, DIRTY_POS)
			| setFlag(0ull, PAGE_SIZE, SIZE_BEGIN, SIZE_END)
			| setFlag(0ull, KEEPER_CACHE_LEVEL, LEVEL_BEGIN, LEVEL_END);

		constexpr static bool getPin(size_t flags) { return getFlag(flags, PIN_POS); }

		constexpr static void setPin(size_t &flags, bool flag = DEFAULT_PIN) { flags = setFlag(flags, flag, PIN_POS); }

		constexpr static bool getAutoload(size_t flags) { return getFlag(flags, AUTOLOAD_POS); }

		constexpr static void setAutoload(size_t &flags, bool flag = DEFAULT_AUTOLOAD) { flags = setFlag(flags, flag, AUTOLOAD_POS); }

		constexpr static bool getAutodump(size_t flags) { return getFlag(flags, AUTODUMP_POS); }

		constexpr static void setAutodump(size_t &flags, bool flag = DEFAULT_AUTODUMP) { flags = setFlag(flags, flag, AUTODUMP_POS); }

		constexpr static bool getDirty(size_t flags) { return getFlag(flags, DIRTY_POS); }

		constexpr static void setDirty(size_t &flags, bool flag = DEFAULT_DIRTY) { flags = setFlag(flags, flag, DIRTY_POS); }

		constexpr static size_t getSize(size_t flags) { return getFlag(flags, SIZE_BEGIN, SIZE_END); }

		constexpr static void setSize(size_t &flags, size_t size = PAGE_SIZE) { flags = setFlag(flags, size, SIZE_BEGIN, SIZE_END); }

		constexpr static size_t getLevel(size_t flags) { return getFlag(flags, LEVEL_BEGIN, LEVEL_END); }

		constexpr static void setLevel(size_t &flags, size_t level = KEEPER_CACHE_LEVEL) { flags = setFlag(flags, level, LEVEL_BEGIN, LEVEL_END); }

		constexpr static bool isValid(size_t flags) { return getLevel(flags) != KEEPER_CACHE_LEVEL; }

		struct VirtualPage : Page {
			Keeper &_keeper;
			address _addr = NULL_ADDRESS;
			size_t _flags; // attribute for virtual page

		public:
			inline VirtualPage(Keeper &keeper, size_t flags = DEFAULT_FLAGS)
				: Page(keeper._caches[getFlag(flags, LEVEL_BEGIN, LEVEL_END)].container())
				, _keeper(keeper), _addr(NULL_ADDRESS), _flags(flags) {
			}

			inline VirtualPage(VirtualPage &other)
				: Page(other), _keeper(other._keeper), _addr(other._addr), _flags(other._flags) {
			}

			inline VirtualPage(VirtualPage &&other)
				: Page(std::move(other)), _keeper(other._keeper), _addr(other._addr), _flags(other._flags) {
				other.reset();
			}

			inline VirtualPage &operator=(const VirtualPage &other) {
				Page::operator=(other);
				_addr = other._addr;
				_flags = other._flags;
				return *this;
			}

			inline VirtualPage &operator=(VirtualPage &&other) {
				if (&_keeper != &other._keeper) {
					throw std::runtime_error("[VirtualPage::operator=]");
				}
				Page::operator=(std::move(other));
				_addr = other._addr;
				_flags = other._flags;
				other.reset();
				return *this;
			}

			// this flag will pin the page in cache immediately after fetching successs, sync with the pin state
			inline bool getPin() const { return Keeper::getPin(_flags); }

			inline void setPin(bool flag = DEFAULT_PIN) { Keeper::setPin(_flags, flag); }

			inline bool getAutoload() const { return Keeper::getAutoload(_flags); } // when hit and page size not enough, autoload is the flag for re-get content from drive/memory

			inline void setAutoload(bool flag = DEFAULT_AUTOLOAD) { Keeper::setAutoload(_flags, flag); }

			inline bool getAutodump() const { return Keeper::getAutodump(_flags); }

			inline void setAutodump(bool flag = DEFAULT_AUTODUMP) { Keeper::setAutodump(_flags, flag); }

			inline bool getDirty() const { return Keeper::getDirty(_flags); }

			inline void setDirty(bool flag = DEFAULT_DIRTY) { Keeper::setDirty(_flags, flag); }

			inline size_t getSize() const { return Keeper::getSize(_flags); }

			inline void setSize(size_t size = PAGE_SIZE) { Keeper::setSize(_flags, size); }

			inline size_t getLevel() const { return Keeper::getLevel(_flags); }

			inline void setLevel(size_t level = KEEPER_CACHE_LEVEL) { Keeper::setLevel(_flags, level); }

			inline operator bool() const { return Keeper::isValid(_flags); }
			
			inline void reset(address addr = NULL_ADDRESS, size_t flags = DEFAULT_FLAGS) { _addr = addr; _flags = flags; }

			inline Keeper::Cache &cache() { return _keeper._caches[getLevel()]; }

			inline bool isPinned() { return getPin(); }

			inline bool pin() { return cache().pin(_addr) && (setPin(true), true); }

			inline bool unpin() { return cache().unpin(_addr) && (setPin(false), true); }
		};

		// wrapper manage only in a procedure, not thread-safe
		// only movable
		template<typename Derived
			, ns::keeper::check_base_t<VirtualPage, Derived> * = nullptr
		> struct PagePtr : std::shared_ptr<Derived> {
			using Super = std::shared_ptr<Derived>;

			bool _tmp = false;

			inline PagePtr(Super &&ptr = nullptr, bool pin = true) : Super(ptr) {
				if (ptr && pin) {
					this->pin();
				}
			}

			PagePtr(const PagePtr &) = delete;

			PagePtr(PagePtr &&other) : Super(std::move(other)), _tmp(other._tmp) {
				other._tmp = false;
			}
			
			inline ~PagePtr() {
				unpin();
			}

			PagePtr &operator=(const PagePtr &) = delete;

			PagePtr &operator=(PagePtr &&other) {
				Super::operator=(std::move(other));
				_tmp = other._tmp;
				other._tmp = false;
				return *this;
			}
			
			inline bool isPinned() { return Super::get()->isPinned(); }

			inline bool pin() {
				auto ptr = Super::get();
				if (!ptr) {
					return false;
				}
				if (isPinned()) {
					return true;
				}
				_tmp = true;
				
				while (ptr->isActive() && !ptr->pin()) {
				} // might be switch out between isActive and pin, so need a loop
				while (!ptr->isActive()) {
					auto flags = ptr->_flags;
					setAutoload(flags, false);
					setPin(flags, true);
					auto tmp = ptr->_keeper.hold<Derived>(ptr->_addr, flags); // TODO: write async collect function to replace code here (avoid data move)
					if (!tmp) {
						return false;
					}
					*tmp = std::move(*ptr); // this one, we can set cache.ptrs with ptr if page miss, and throw exception when hit with collect
					Super::operator=(std::move(tmp));
				}
				return true;
			}

			inline bool unpin() {
				auto ptr = Super::get();
				if (!ptr) {
					return false;
				}
				if (isPinned() && _tmp) {
					ptr->unpin();
					_tmp = false;
				}
				return true;
			}
		};

		Drive _drive;
		Translator _translator;
		std::array<Cache, KEEPER_CACHE_LEVEL> _caches;
		// TODO: replace tasks with promise and argument calling list ?
		// TODO: design or use lock-free deque data structure

		bool _open = false;
		std::mutex _openMutex;

		std::deque<std::packaged_task<std::shared_ptr<VirtualPage>()>> _tasks;
		std::mutex _tasksMutex;
		std::condition_variable _tasksNotEmpty;

		std::thread _loopThread;
		
		inline Keeper() : _drive(), _translator(_drive)
			, _caches{ Cache(*this), Cache(*this), Cache(*this) } { // TODO: ugly initialization
		}

		inline Keeper(const char * path, bool truncate = false) : Keeper() {
			open(path, truncate);
		}

		inline Keeper(const std::string &path, bool truncate = false) : Keeper(path.c_str(), truncate) {
		}

		inline ~Keeper() {
			close();
		}

		inline bool isOpen() {
			std::unique_lock<std::mutex> lock(_openMutex);
			return _open;
		}

		inline void open(const char * path, bool truncate = false) {
			std::unique_lock<std::mutex> lock(_openMutex);
			if (_open || _drive.isOpen() || _translator.isOpen()) {
				throw std::runtime_error("[Keeper::open]");
			}

			_drive.open(path, truncate);
			_translator.open(TRANSLATOR_LOOKASIDE_SIZE);
			size_t i = 0;
			for (auto &cache : _caches) {
				cache.open(KEEPER_CACHE_SIZES[i++]);
			}

			_loopThread = std::thread([this]() { loop(); });
			_open = true;
		}

		inline void close() {
			std::unique_lock<std::mutex> lock(_openMutex);
			if (!_open) {
				return;
			}
			_open = false;
			lock.unlock();

			_loopThread.join();

			lock.lock();
			for (auto &cache : _caches) {
				cache.close();
			}
			_translator.close();
			_drive.close();
		}

		inline string &name() { return _translator.name(); }

		inline size_t &param(address addr) { return _translator.param(addr); }

		// IMPORTANT: same rule: partial load should be read only
		template<typename Derived
			, ns::cache::check_base_t<VirtualPage, Derived> * = nullptr
			, typename... Args
		> inline Derived onCreate(size_t flags, const Args &... args) {
			return Derived(*this, flags);
		}

		// don't auto load and save in handler
		// IMPORTANT: get and put softly: if the address is not allocate, return a memory page without allocation
		// and allocate the address when it have to be write back
		inline bool onInsert(address addr, Page &page, size_t flags = DEFAULT_FLAGS) {
			if (!isValid(flags)) {
				flags = reinterpret_cast<VirtualPage *>(&page)->_flags;
			}
			reinterpret_cast<VirtualPage *>(&page)->_addr = addr;
			address size = getSize(flags);
			if (size != PAGE_SIZE) {
				page.resize(size);
			}

			bool load = getAutoload(flags);
			bool flag = false;
			auto result = _translator(addr, flag);
			if (flag) {
				return _drive.get(page, result, load);
			} else { // clean all the content as it is newly allocated by file system
				page.clear();
				return !load || page.load();
			}
		}

		inline bool onHit(address addr, Page &page, size_t flags = DEFAULT_FLAGS) {
			if (!isValid(flags)) {
				flags = reinterpret_cast<VirtualPage *>(&page)->_flags;
			}

			if (page.size() < getSize(flags)) {
				return onInsert(addr, page, flags);
			} else {
				auto ptr = reinterpret_cast<VirtualPage *>(&page);
				if (getAutodump(flags)) {
					ptr->setAutodump(true);
				}
				if (getDirty(flags)) {
					ptr->setDirty(true);
				}
				return true;
			}
		}

		inline bool onErase(address addr, Page &page, size_t flags = DEFAULT_FLAGS) {
			flags = reinterpret_cast<VirtualPage *>(&page)->_flags; // param flags are for insert and hit
			
			if (getSize(flags) != PAGE_SIZE || !getDirty(flags)) { // clean and partial page don't need to dump and put
				return true;
			}

			bool flag = false;
			auto result = _translator(addr, flag);
			if (!flag) {
				result = _drive.allocate(addr, flag);
				flag = _translator.link(addr, result);
			}
			return flag && _drive.put(page, result, getAutodump(flags));
		}

		template<typename Derived
			, ns::keeper::check_base_t<VirtualPage, Derived> * = nullptr
		> inline std::shared_ptr<VirtualPage> holdSync(address addr, size_t flags) {
			std::shared_ptr<Derived> ptr;
			try {
				ptr = _caches[getLevel(flags)].fetch<Derived>(addr, flags);
			} catch (const std::runtime_error &e) {
				std::cerr << e.what() << std::endl;
				return nullptr;
			}

			if (getPin(flags)) { // handle pin flag
				if (!ptr->pin()) {
					throw std::runtime_error("[Keeper::holdSync]");
				}
			}

			return std::static_pointer_cast<VirtualPage>(ptr);
		}
		
		inline std::shared_ptr<VirtualPage> loosenSync(address addr) {
			try {
				auto &cache = _caches[getCacheLevel(addr)];
				if (cache._map.find(addr) != cache._map.end()) {
					if (cache.isPinned(addr)) {
						throw std::runtime_error("[Keeper::loosenSync]");
					} else {
						cache.discard(addr);
					}
				}
				if (!_translator.unlink(addr)) {
					// throw std::runtime_error("Keeper::loosenSync"); // DEBUG
				}
			} catch (const std::runtime_error &e) {
				std::cerr << e.what() << std::endl;
			}
			return nullptr; // return value for _tasks structure convenience
		}

		inline void term(int count = 1) {
			while (true) {
				std::unique_lock<std::mutex> lock(_tasksMutex);
				if (_tasks.empty()) {
					if (!count--) {
						return;
					}
					using namespace std::chrono_literals;
					_tasksNotEmpty.wait_for(lock, 50ms);
				} else {
					auto task = std::move(_tasks.front());
					_tasks.pop_front();
					lock.unlock();
					task();
				}
			}
		}

		inline void loop() {
			while (isOpen()) {
				term(1);
			}
			term(0);
		}

		inline bool addTask(std::packaged_task<std::shared_ptr<VirtualPage>()> &&task) {
			if (!isOpen()) {
				return false;
			}

			std::unique_lock<std::mutex> lock(_tasksMutex);
			_tasks.push_back(std::move(task));
			_tasksNotEmpty.notify_all();
			return true;
		}

		template<typename Derived
			, ns::keeper::check_base_t<VirtualPage, Derived> * = nullptr
		> inline std::future<std::shared_ptr<VirtualPage>> holdAsync(address addr, size_t flags) {
			std::packaged_task<std::shared_ptr<VirtualPage>()> task([this, addr, flags]() {
				return holdSync<Derived>(addr, flags);
			});

			auto result = task.get_future();
			if (!addTask(std::move(task))) {
				throw std::runtime_error("[Keeper::holdAsync]");
			}
			return result;
		}

		inline std::future<std::shared_ptr<VirtualPage>> loosenAsync(address addr) {
			std::packaged_task<std::shared_ptr<VirtualPage>()> task([this, addr]() {
				return loosenSync(addr);
			});

			auto result = task.get_future();
			if (!addTask(std::move(task))) {
				throw std::runtime_error("[Keeper::holdAsync]");
			}
			return result;
		}

		template<typename Derived
			, ns::keeper::check_base_t<VirtualPage, Derived> * = nullptr
		> inline PagePtr<Derived> hold(address addr, size_t flags) {
			//auto temp = !getPin(flags);
			//setPin(flags, true);
			auto result = holdAsync<Derived>(addr, flags);
			result.wait();
			return PagePtr(std::static_pointer_cast<Derived>(result.get()));
			//ptr._tmp = temp;
			//return ptr;
		}

		template<typename Derived
			, ns::keeper::check_base_t<VirtualPage, Derived> * = nullptr
		> inline PagePtr<Derived> hold(address addr, bool load = DEFAULT_AUTOLOAD, bool dump = DEFAULT_AUTODUMP
			, bool dirty = DEFAULT_DIRTY, bool pin = DEFAULT_PIN, address size = PAGE_SIZE) {
			// encode flags
			size_t flags = setFlag(0ull, pin, PIN_POS)
				| setFlag(0ull, load, AUTOLOAD_POS)
				| setFlag(0ull, dump, AUTODUMP_POS)
				| setFlag(0ull, dirty, DIRTY_POS)
				| setFlag(0ull, size, SIZE_BEGIN, SIZE_END)
				| setFlag(0ull, getCacheLevel(addr), LEVEL_BEGIN, LEVEL_END);
			return hold<Derived>(addr, flags);
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

	template<typename Derived>
	using PagePtr = Keeper::PagePtr<Derived>;
}
