#pragma once

#include "definitions.hpp"
#include "page.hpp"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

namespace db {
	// DESIGN:
	// use unformatted input to read and write
	// keep a simple page buffer to avoid read-write on the same page continuously
	// container should keep as a sequencial struture like vector to use unformatted read-write
	// TODO: keep open status sync with every member variables
	struct DriveBuffer {
		using Path = std::filesystem::path;

		constexpr static auto NORMAL_MODE = std::ios::in | std::ios::out | std::ios::binary;
		constexpr static auto CREATE_MODE = std::ios::out | std::ios::binary;
		
		constexpr static size_t BUFFER_PAGE_POS = 0;

		Path _path;
		drive_address _size = 0;
		std::fstream _stream;
		Container _fixed; // fixed container shared by DriveBuffer & Drive & Translater - store all fixed load page
		Page _buffer;
		drive_address _current = NULL_ADDRESS;
		bool _ioFlag = false; // false means last io is read, true means last io is write

	public:
		inline DriveBuffer(): _fixed(BUFFER_PAGE_POS + PAGE_SIZE), _buffer(_fixed) {
		}

		inline DriveBuffer(const char * path, bool truncate = false) : DriveBuffer() {
			open(path, truncate);
		}

		inline DriveBuffer(const std::string &path, bool truncate = false) : DriveBuffer() {
			open(path, truncate);
		}

		inline ~DriveBuffer() {
			close();
		}

		inline bool isOpen() const {
			return !_path.empty();
		}

		inline void open(const char *path, bool truncate = false) {
			if (isOpen()) {
				throw std::runtime_error("[DriveBuffer::open]");
			}

			// create file if not exists
			_path = path;
			namespace fs = std::filesystem;
			auto t = fs::status(_path).type();
			if (t != fs::file_type::regular) {
				if (t == fs::file_type::none || t == fs::file_type::not_found) {
					_stream.open(_path, CREATE_MODE);
					_stream.close();
				} else {
					throw std::runtime_error("[DriveBuffer::open]");
				}
			}
			// truncate operation if needed
			_size = static_cast<drive_address>(std::filesystem::file_size(_path));
			if (truncate && _size) {
				clear();
			}

			_stream.open(_path, NORMAL_MODE);

			//_stream.unsetf(std::ios_base::skipws); // important trap the default >> will skip writespace when copy
			//std::istream_iterator<char> in(_stream);
			//std::copy_n(in, p.size(), p.begin());
		}
		
		inline void open(const std::string &path, bool truncate = false) {
			open(path.c_str(), truncate);
		}

		inline void close() {
			if (!isOpen()) {
				return;
			}
			if (!flush()) {
				throw std::runtime_error("[DriveBuffer::close]");
			}
			_current = NULL_ADDRESS;
			_stream.close();
			_ioFlag = false;
			_size = 0;
			_path.clear();
		}

		inline const Path &path() { return _path; }

		inline drive_address size() { return _size; }

		inline Container &appendFixed(size_t size = 0) {
			_fixed.resize(_fixed.size() + size);
			return _fixed;
		}

		inline size_t fixedSize() { return _fixed.size(); }

		// default for clear
		inline void resize(drive_address size) {
			std::filesystem::resize_file(_path, size);
			_size = size;
		}

		inline void clear() { resize(0); }

		inline void expand(drive_address size = EXPAND_SIZE) { resize(_size + size); }

		inline void shrink(drive_address size = SHRINK_SIZE) {
			if (_size < size) {
				throw std::runtime_error("[DriveBuffer::shrink]");
			}
			resize(_size - size);
		}

		inline bool flush() {
			if (!_buffer.isActive()) {
				return true;
			}
			if (_stream.tellp() != _current) {
				_stream.seekp(_current);
			}
			if (_stream.write(_buffer.data(), _buffer.size())) {
				_stream.flush();
				_buffer.deactivate();
				_ioFlag = true;
				return true;
			} else {
				return false;
			}
		}

		inline bool get(Page &page, drive_address addr, bool load = true) {
			if (addr % PAGE_SIZE) { // physical address doesn't align to page size
				return false;
			}
			size_t pos = 0, last = page.size();
			if (addr == _current && _buffer.isActive()) {
				pos = _buffer.size();
				if (last <= pos) {
					std::copy_n(_buffer.begin(), last, page.begin());
					return !load || page.load();
				} else {
					std::copy_n(_buffer.begin(), pos, page.begin());
				}
			}
			if (_ioFlag || _stream.tellg() != addr + pos) {
				_stream.seekg(addr + pos);
				_ioFlag = false;
			}
			_stream.read(page.data() + pos, last - pos);
			if (!_stream) {
				return false;
			}
			return !load || page.load();

			//if (sync) {
			//	_stream.sync(); // TODO: find better sync time
			//}
		}

		inline bool put(Page &page, drive_address addr, bool dump = true) {
			if (addr % PAGE_SIZE) { // physical address doesn't align to page size
				return false;
			}
			if (dump && !page.dump()) {
				return false;
			}
			if (addr == _current && _buffer.isActive()) {
				if (_buffer.size() < page.size()) {
					_buffer.resize(page.size());
				}
			} else {
				if (flush()) {
					_buffer.activate(BUFFER_PAGE_POS, BUFFER_PAGE_POS + page.size());
				} else {
					return false;
				}
			}
			std::copy(page.begin(), page.end(), _buffer.begin());
			_current = addr;
			return true;
		}
	};

	// TODO: better name design and replace saving vectors' end address with size for better comprehension
	// TODO: use handler mechanism to avoid vector generator
	struct DriveEntryPage : Page {
		constexpr static size_t TOTAL_SIZE_POS = 0;
		constexpr static size_t FREE_SIZE_POS = TOTAL_SIZE_POS + sizeof(drive_address);
		constexpr static size_t SYSTEM_MASTERS_SIZE_POS = 1020;
		constexpr static size_t USER_MASTERS_SIZE_POS = SYSTEM_MASTERS_SIZE_POS + sizeof(page_address);

		constexpr static size_t SYSTEM_MASTERS_BEGIN = USER_MASTERS_SIZE_POS + sizeof(page_address);
		constexpr static size_t SYSTEM_MASTERS_END = SYSTEM_MASTERS_BEGIN + 256;
		constexpr static size_t SYSTEM_MASTERS_CAPACITY = (SYSTEM_MASTERS_END - SYSTEM_MASTERS_BEGIN) / sizeof(drive_address);

		constexpr static size_t USER_MASTERS_BEGIN = SYSTEM_MASTERS_END;
		constexpr static size_t USER_MASTERS_END = 4096;
		constexpr static size_t USER_MASTERS_CAPACITY = (USER_MASTERS_END - USER_MASTERS_BEGIN) / sizeof(drive_address);

		drive_address _totalSize; // [0,8)
		drive_address _freeSize; // [8,16)

		// TODO: other timestamp value, like last sync at, ...
		// timestamp syncAt;

		// page_address SYSTEM_MASTERS_END [1020, 1022)
		// page_address USER_MASTERS_END [1022, 1024)
		std::vector<drive_address> _systemMasters; // [1024, 2048)
		std::vector<drive_address> _userMasters; // [2048, 4096)

	public:
		inline DriveEntryPage(Container &container) : Page(container) {
		}

		virtual bool load() {
			_totalSize = read<drive_address>(TOTAL_SIZE_POS);
			_freeSize = read<drive_address>(FREE_SIZE_POS);

			_systemMasters.resize(read<page_address>(SYSTEM_MASTERS_SIZE_POS));
			_userMasters.resize(read<page_address>(USER_MASTERS_SIZE_POS));
			for (size_t i = 0; i != _systemMasters.size(); ++i) {
				_systemMasters[i] = read<drive_address>(SYSTEM_MASTERS_BEGIN + i * sizeof(drive_address));
			}
			for (size_t i = 0; i != _userMasters.size(); ++i) {
				_userMasters[i] = read<drive_address>(USER_MASTERS_BEGIN + i * sizeof(drive_address));
			}
			return true;
		}

		virtual bool dump() {
			if (_systemMasters.size() > SYSTEM_MASTERS_CAPACITY || _userMasters.size() > USER_MASTERS_CAPACITY) {
				return false;
				// throw std::runtime_error("[DriveEntryPage::dump]");
			}
			write(_totalSize, TOTAL_SIZE_POS);
			write(_freeSize, FREE_SIZE_POS);

			write(static_cast<page_address>(_systemMasters.size()), SYSTEM_MASTERS_SIZE_POS);
			write(static_cast<page_address>(_userMasters.size()), USER_MASTERS_SIZE_POS);

			auto i = SYSTEM_MASTERS_BEGIN;
			for (auto m : _systemMasters) {
				write(m, i);
				i += sizeof(drive_address);
			}
			i = USER_MASTERS_BEGIN;
			for (auto m : _userMasters) {
				write(m, i);
				i += sizeof(drive_address);
			}
			return true;
		}
	};

	// TODO: use int<24> to replace offset ptr value type for better perform
	struct DriveMasterPage : Page {
		using address_offset = std::int16_t;

		constexpr static page_address FORWORD_POS = 0;
		constexpr static page_address BACK_POS = FORWORD_POS + sizeof(drive_address);
		constexpr static page_address SLAVES_SIZE_POS = BACK_POS + sizeof(drive_address);
		constexpr static page_address HEADER_SIZE = SLAVES_SIZE_POS + sizeof(page_address);
		
		constexpr static page_address SLAVES_END = PAGE_SIZE;
		constexpr static page_address SLAVES_CAPACITY = (SLAVES_END - HEADER_SIZE) / static_cast<page_address>(sizeof(address_offset));
		constexpr static page_address SLAVES_BEGIN = SLAVES_END - SLAVES_CAPACITY * static_cast<page_address>(sizeof(address_offset));
		
		drive_address _forward; // [0, 8)
		drive_address _back; // [8, 16)
		size_t _size = 0; // [16, 18) slave size, important in partial page, and should keep consistent with _slaves.size()

		std::vector<address_offset> _slaves; // [18, 4096)

		inline DriveMasterPage(Container &container) : Page(container), _forward(NULL_ADDRESS), _back(NULL_ADDRESS) {
		}

		virtual bool load() {
			_forward = read<drive_address>(FORWORD_POS);
			_back = read<drive_address>(BACK_POS);
			_size = read<page_address>(SLAVES_SIZE_POS);
			// partial load
			if (size() == HEADER_SIZE) {
				return true;
			}

			_slaves.resize(_size);
			size_t i = 0;
			for (auto &s : _slaves) {
				s = read<address_offset>(SLAVES_BEGIN + i * sizeof(address_offset));
				++i;
			}
			return true;
		}

		virtual bool dump() {
			if (size() != PAGE_SIZE || _slaves.size() > SLAVES_CAPACITY) {
				// partial dump is not allowed
				return false;
			}

			write(_forward, FORWORD_POS);
			write(_back, BACK_POS);
			write(static_cast<page_address>(_size), SLAVES_SIZE_POS);
			auto i = SLAVES_BEGIN;
			for (auto s : _slaves) {
				write(s, i);
				i += sizeof(address_offset);
			}
			return true;
		}
	};

	// TODO: don't need anything for now, it should get to work when add sweeper
	// TODO: for now we don't consider machine error problems, so we don't have extra check on all the data
	struct DriveSlavePage : Page {
		// drive_address master_ptr;
		inline DriveSlavePage(Container &container) : Page(container) {
		}
		/*virtual void load() {
			master_ptr = read<drive_address>(MASTER_PTR_POS);
		}

		virtual void dump() {
			write(master_ptr, MASTER_PTR_POS);
		}*/
	};

	// cache is used for free page management, so only drive free structure pages are loaded
	// partial load should be read-only, changes on partial load data will be discard
	// allocator will keep the last master in chain in memory and masters when dump
	struct DriveAllocator : BasicCacheHandler<drive_address, Page> {
		using address_offset = typename DriveMasterPage::address_offset;
		using Cache = db::Cache<drive_address, Page, DriveAllocator>;

		// if invalid return NULL_ADDRESS
		constexpr static address_offset slaveOffset(drive_address slave, drive_address master) {
			auto offset = static_cast<address_offset>(slave / PAGE_SIZE - master / PAGE_SIZE);
			return master + offset * PAGE_SIZE == slave ? offset : static_cast<address_offset>(NULL_ADDRESS);
		}

		DriveBuffer &_buffer;
		std::vector<drive_address> &_masters;
		Cache _cache;
		drive_address _expandSize;
		drive_address _shrinkSize;

		inline DriveAllocator(DriveBuffer &buffer, std::vector<drive_address> &masters, drive_address expandSize = EXPAND_SIZE, drive_address shrinkSize = SHRINK_SIZE)
			: _buffer(buffer), _masters(masters), _cache(*this), _expandSize(expandSize), _shrinkSize(shrinkSize) {
		}

		inline ~DriveAllocator() {
			close();
		}
		
		inline bool isOpen() {
			return _cache.isOpen();
		}

		inline void open(size_t capacity) {
			_cache.open(capacity);
		}

		inline void close() {
			if (!isOpen()) {
				return;
			}
			dump(true);
			_cache.close();
		}

		inline void load() {
			for (auto addr : _masters) {
				_cache.fetch<DriveMasterPage>(addr, DriveMasterPage::HEADER_SIZE);
			}
			if (!_masters.empty()) {
				_cache.pin(_masters.back());
			}
		}

		inline void dump(bool unpin = false) {
			// TODO: access private member funciton
			if (!_masters.empty()) {
				_cache.unpin(_masters.back());
			}
			_masters.resize(_cache.size());
			size_t j = 0;
			for (auto &p : _cache._map) {
				_masters[j++] = p.first;
			}
			std::sort(_masters.begin(), _masters.end());
			if (!unpin && !_masters.empty()) {
				_cache.pin(_masters.back());
			}
		}

		inline void elastic(drive_address expandSize, drive_address shrinkSize) {
			_expandSize = expandSize;
			_shrinkSize = shrinkSize;
		}

		template<typename DerivedPage, typename... Args>
		inline DerivedPage onCreate(const Args &... args) {
			return DerivedPage(_cache.container());
		}

		inline bool onHit(drive_address addr, Page &page, size_t size = PAGE_SIZE) {
			// TODO: don't know how to get length from onHit parameters
			if (page.size() < size) {
				page.resize(size);
				return _buffer.get(page, addr);
			}
			return true;
		}

		// handle Cache insert and put insert mapping value in &value
		inline bool onInsert(drive_address addr, Page &page, size_t size = PAGE_SIZE) {
			if (size != PAGE_SIZE) {
				page.resize(size);
			}
			return _buffer.get(page, addr);
		}

		// handle Cache erase and put mapping value in &value for cleaning/write_back ...
		inline bool onErase(drive_address addr, Page &page, size_t size = PAGE_SIZE) {
			return page.size() != PAGE_SIZE || _buffer.put(page, addr);
		}

		void insert(drive_address addr) {
			// find start master page and traversing bound
			// pos is set to NULL_ADDRESS when it might insert at the end
			auto left = NULL_ADDRESS; // left bound for traverse
			auto right = NULL_ADDRESS; // right bound for traverse
			auto pos = NULL_ADDRESS; // traverse result if we have to insert a master, pos is the back master of the new one, NULL_ADDRESS means insert at the back of the master chain
			if (!_masters.empty()) {
				auto iter = std::lower_bound(_masters.begin(), _masters.end(), addr);
				if (iter == _masters.end()) {
					right = *(iter - 1);
					pos = NULL_ADDRESS;
					left = right;
				} else {
					right = *iter;
					pos = right;
					if (iter == _masters.begin()) {
						left = NULL_ADDRESS;
					} else {
						left = *(iter - 1);
					}
				}
			}

			// traverse part of chain
			drive_address next = NULL_ADDRESS;
			while (right != NULL_ADDRESS) {
				if (right == addr) {
					throw std::runtime_error("[Drive::insert]");
				}
				auto offset = slaveOffset(addr, right);
				if (offset != NULL_ADDRESS) { // check if distance fits in address_offset
					auto ptr = _cache.fetch<DriveMasterPage>(right, DriveMasterPage::HEADER_SIZE);
					if (ptr->_size < DriveMasterPage::SLAVES_CAPACITY) { // TOOD: shit code, need resharp
						// auto sptr = cache.fetch<DriveSlavePage>(addr);
						// put(slave, addr);
						ptr = _cache.fetch<DriveMasterPage>(right);
						insertSlave(*ptr, offset);
						if (next != NULL_ADDRESS) {
							// traverse at least one page might not on the cache, so the cache addresses might be changed
							// so we should sync _cache stored addresses with masters for the next search
							dump();
						}
						return;
					} else {
						next = ptr->_forward;
					}
				} else { // offset overflow
					if (right < addr) { // right is too small
						break; // not need to continue searching
					}
					auto ptr = _cache.fetch<DriveMasterPage>(right, DriveMasterPage::HEADER_SIZE);
					next = ptr->_forward;
				}
				if (right == left) {
					break;
				}
				right = next;
				if (addr < right) {
					pos = right;
				}
			}

			insertMaster(addr, pos);
			dump();
		}

		inline void insertMaster(drive_address addr, drive_address pos) {
			Container tmp(PAGE_SIZE);
			DriveMasterPage page(tmp);
			page.activate(0, PAGE_SIZE);

			if (pos == NULL_ADDRESS) {
				if (!_masters.empty()) {
					auto fwd = _masters.back();
					if (fwd != NULL_ADDRESS) {
						page._forward = fwd;
						auto ptr = _cache.fetch<DriveMasterPage>(fwd);
						ptr->_back = addr;
						_cache.unpin(fwd);
					}
				}
				_buffer.put(page, addr);
				// if insert at the back, fetch it and pin in memory
				_cache.fetch<DriveMasterPage>(addr);
			} else {
				auto ptr = _cache.fetch<DriveMasterPage>(pos);
				auto fwd = ptr->_forward;
				page._forward = fwd;
				page._back = pos;
				ptr->_forward = addr;
				if (fwd != NULL_ADDRESS) {
					ptr = _cache.fetch<DriveMasterPage>(fwd);
					ptr->_back = addr;
				}
				_buffer.put(page, addr);
			}
		}

		inline void insertSlave(DriveMasterPage &master, address_offset offset) {
			auto &slaves = master._slaves;
			auto iter = std::lower_bound(slaves.begin(), slaves.end(), offset);
			slaves.insert(iter, offset);
			master._size = slaves.size();
		}

		// erase is used for allocation, so we don't have to erase the actual address free page, but the one that is approximately near
		// return the address actually erased (it is a private function for allocate), the address allocate just need to be close enough to the target
		// key point is to avoid I/O
		// must have free page to erase (masters not empty)
		drive_address erase(drive_address addr) {
			auto iter = std::lower_bound(_masters.begin(), _masters.end(), addr);
			for (auto i = 0; i != 2; ++i, --iter) {
				if (iter == _masters.end()) {
					continue;
				}
				auto offset = slaveOffset(addr, *iter);
				if (offset != NULL_ADDRESS) {
					auto ptr = _cache.fetch<DriveMasterPage>(*iter);
					auto result = eraseSlave(*ptr, *iter, offset);
					if (result != NULL_ADDRESS) {
						return result;
					}
				}
				if (iter == _masters.begin()) {
					break;
				}
			}
			auto result = *iter;
			eraseMaster(*iter);
			dump();
			return result;
		}

		inline void eraseMaster(drive_address addr) {
			auto ptr = _cache.fetch<DriveMasterPage>(addr);
			if (addr == _masters.back()) { // can use ptr->_back to check instead
				auto fwd = ptr->_forward;
				_cache.unpin(addr);
				_cache.discard(addr);
				if (fwd != NULL_ADDRESS) {
					ptr = _cache.fetch<DriveMasterPage>(fwd);
					ptr->_back = NULL_ADDRESS;
				}
			} else {
				auto fwd = ptr->_forward, bck = ptr->_back;
				_cache.discard(addr);
				ptr = _cache.fetch<DriveMasterPage>(bck);
				ptr->_forward = fwd;
				if (fwd != NULL_ADDRESS) {
					ptr = _cache.fetch<DriveMasterPage>(fwd);
					ptr->_back = bck;
				}
			}
		}

		inline drive_address eraseSlave(DriveMasterPage &master, drive_address m, address_offset offset) {
			auto &slaves = master._slaves;
			if (slaves.empty()) {
				return NULL_ADDRESS;
			}
			auto iter = std::lower_bound(slaves.begin(), slaves.end(), offset);
			if (iter == slaves.end()) {
				--iter;
			}
			auto result = m + *iter * PAGE_SIZE;
			slaves.erase(iter);
			master._size = slaves.size();
			return result;
		}

		inline drive_address allocate(drive_address addr = 0) {
			if (_masters.empty()) {
				auto i = _buffer.size();
				_buffer.expand(_expandSize);
				auto size = _buffer.size();
				for (; i < size; i += PAGE_SIZE) {
					insert(i);
				}
			}
			return erase(addr);
		}

		inline void free(drive_address addr) {
			insert(addr);
			// TODO: shrink
		}
	};

	// sync controller for database io management
	// TOOO: DriveBuffer, Drive, Translater's fix part on same container (need to pass constant and container reference and other staff)
	// TODO: optimize the cache replacement for this scenario, which fetch will case a partial traverse, and only two cache page is involved, so we don't need to keep searching the free block
	// use a tree set to get lower_bound of an address
	// the first and the last master page is pinned until close
	// no need to pin-unpin in a function, because it works in one thread, but pin the last master page for the convinience
	// TODO: allocate and insert block
	struct Drive : DriveBuffer {
		using address_offset = typename DriveMasterPage::address_offset;
		using Cache = Cache<drive_address, Page, Drive>;

		DriveEntryPage _entry;
		DriveAllocator _system;
		DriveAllocator _user;

		inline Drive() : DriveBuffer(), _entry(appendFixed(PAGE_SIZE))
			, _system(*this, _entry._systemMasters, EXPAND_SYSTEM_SIZE, SHRINK_SYSTEM_SIZE)
			, _user(*this, _entry._userMasters, EXPAND_USER_SIZE, SHRINK_USER_SIZE) {
			_entry.activate(fixedSize() - PAGE_SIZE, fixedSize());
		}

		inline Drive(const char * path, bool truncate = false) : Drive() {
			open(path, truncate);
		}

		inline Drive(const std::string &path, bool truncate = false) : Drive(path.c_str(), truncate) {
		}

		inline ~Drive() {
			close();
		}
		
		// derive bool isOpen()

		inline void open(const char * path, bool truncate = false) {
			DriveBuffer::open(path, truncate);
			_system.open(DriveEntryPage::SYSTEM_MASTERS_CAPACITY);
			_user.open(DriveEntryPage::USER_MASTERS_CAPACITY);
			if (size()) {
				load();
			} else {
				init();
			}
		}
		
		inline void open(const std::string &path, bool truncate = false) {
			open(path.c_str(), truncate);
		}

		inline void close() {
			if (!isOpen()) {
				return;
			}
			_user.close();
			_system.close();
			dump();
			DriveBuffer::close();
		}

		inline void load() {
			get(_entry, FIXED_DRIVE_ENTRY_PAGE);
			if (_entry._totalSize != size()) { // TODO: simple verify
				throw std::runtime_error("[Drive::load]");
			}
			_system.load();
			_user.load();
		}

		inline void dump() {
			_entry._totalSize = size();
			put(_entry, FIXED_DRIVE_ENTRY_PAGE);
		}

		inline void init() {
			expand(INIT_SIZE);
			get(_entry, FIXED_DRIVE_ENTRY_PAGE, false);
			for (auto i = FIXED_SIZE; i < FIXED_SIZE + INIT_SYSTEM_SIZE; i += PAGE_SIZE) {
				free(i, true);
			}
			for (auto i = FIXED_SIZE + INIT_SYSTEM_SIZE; i < INIT_SIZE; i += PAGE_SIZE) {
				free(i);
			}
			// _entry._freeSize = 0;
		}

		inline drive_address allocate(drive_address addr = 0, bool system = false) {
			// _entry._freeSize -= PAGE_SIZE; TODO: statistic disabled for now
			return system ? _system.allocate(addr) : _user.allocate(addr);
		}

		inline void free(drive_address addr, bool system = false) {
			// _entry._freeSize += PAGE_SIZE;
			if (system) {
				_system.free(addr);
			} else {
				_user.free(addr);
			}
		}
	};
}
