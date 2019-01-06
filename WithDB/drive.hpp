#pragma once

#include "page.hpp"
#include "definitions.hpp"

#include <algorithm>
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
		using path_type = std::filesystem::path;
		
		constexpr static auto CREATE_MODE = std::ios::out | std::ios::binary;
		constexpr static auto DEFAULT_MODE = std::ios::in | std::ios::out | std::ios::binary;
		constexpr static std::size_t BUFFER_PAGE_POS = 0;

		path_type _path;
		drive_address _size;
		std::fstream _stream;
		Container _container;
		Page _buffer;
		drive_address _address;

	public:
		inline DriveBuffer(size_t fixed = PAGE_SIZE): _size(0), _container(fixed), _buffer(_container), _address(0) {
		}

		inline DriveBuffer(const char * filename, bool truncate = false, size_t fixed = PAGE_SIZE) : DriveBuffer(fixed) {
			open(filename, truncate);
		}
		inline DriveBuffer(const std::string &filename, bool truncate = false, size_t fixed = PAGE_SIZE) : DriveBuffer(fixed) {
			open(filename.c_str(), truncate);
		}

		inline ~DriveBuffer() {
			close();
		}

		inline bool isOpen() const {
			return !_path.empty();
		}

		inline void open(const char *path, bool truncate = false) {
			// check if the file exists
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
			_size = static_cast<drive_address>(std::filesystem::file_size(_path));
			if (truncate && _size) {
				resize(0);
			}
			_stream.open(_path, DEFAULT_MODE);
			// _stream.unsetf(std::ios_base::skipws); // important trap the default >> will skip writespace when copy 
		}
		inline void open(const std::string &path, bool truncate = false) {
			open(path.c_str(), truncate);
		}

		inline void close() {
			if (!isOpen()) {
				return;
			}
			flush();
			_address = 0;
			_stream.close();
			_size = 0;
			_path.clear();
		}

		inline const path_type &path() {
			return _path;
		}

		inline drive_address size() {
			return _size;
		}

		// default for clear
		inline void resize(drive_address size) {
			std::filesystem::resize_file(_path, size);
			_size = size;
		}

		inline void clear() {
			resize(0);
		}

		inline void expand(drive_address size = EXPAND_SIZE) {
			resize(_size + size);
		}

		inline void shrink(drive_address size = SHRINK_SIZE) {
			if (_size < size) {
				throw std::runtime_error("[DriveBuffer::shrink]");
			}
			resize(_size - size);
		}

		inline bool flush() {
			if (!_buffer.isActive()) {
				return false;
			}
			if (_stream.tellp() != _address) {
				_stream.seekp(_address);
			}
			_stream.write(_buffer.data(), _buffer.size());
			_buffer.deactivate();
			return true;
		}

		inline bool get(Page &p, drive_address addr, bool load = true) {
			//if (sync) {
			//	_stream.sync(); // TODO: find better sync time
			//}

			//std::istream_iterator<char> in(_stream);
			//std::copy_n(in, p.size(), p.begin());
			if (addr % PAGE_SIZE) {
				// throw std::runtime_error("[DriveBuffer::get]");
				return false;
			}
			if (addr == _address && _buffer.isActive()) {
				std::copy_n(_buffer.begin(), p.size(), p.begin());
				
			} else {
				if (flush() || _stream.tellg() != addr) {
					_stream.seekg(addr);
				}
				_stream.read(p.data(), p.size());
				if (!_stream) {
					return false;
				}
			}
			return !load || p.load();
		}

		inline bool put(Page &p, drive_address addr, bool dump = true) {
			if (addr % PAGE_SIZE || p.size() != PAGE_SIZE) { // forbid partial put // TODO: maybe it's a bad idea
				// throw std::runtime_error("[DriveBuffer::put] physical address doesn't align to page size");
				return false;
			}
			if (dump && !p.dump()) {
				return false;
			}
			if (addr != _address && _buffer.isActive()) {
				flush();
			}
			_buffer.activate(BUFFER_PAGE_POS, BUFFER_PAGE_POS + PAGE_SIZE);
			std::copy(p.begin(), p.end(), _buffer.begin());
			_address = addr;
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
		constexpr static size_t SYSTEM_MASTERS_END = 2048;
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
		inline DriveEntryPage(Container &container, size_t first, size_t last) : Page(container, first, last) {
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
		constexpr static page_address HEADER_SIZE = BACK_POS + sizeof(drive_address);
		constexpr static page_address SLAVES_SIZE_POS = HEADER_SIZE;
		constexpr static page_address SLAVES_BEGIN = SLAVES_SIZE_POS + sizeof(drive_address);
		constexpr static page_address SLAVES_END = 4096;
		constexpr static page_address SLAVES_CAPACITY = (SLAVES_END - SLAVES_BEGIN) / static_cast<page_address>(sizeof(address_offset));

		drive_address _forward; // [0, 8)
		drive_address _back; // [8, 16)
		// page_address free_slave_offsets_end [16, 18)

		std::vector<address_offset> _slaves; // [18, 4096)

		inline DriveMasterPage(Container &container, size_t first, size_t last) : Page(container, first, last) {
			_forward = FIXED_DRIVE_ENTRY_PAGE;
			_back = FIXED_DRIVE_ENTRY_PAGE;
		}

		virtual bool load() {
			_forward = read<drive_address>(FORWORD_POS);
			_back = read<drive_address>(BACK_POS);

			// partial load
			if (size() == HEADER_SIZE) {
				return true;
			}

			_slaves.resize(read<page_address>(SLAVES_SIZE_POS));
			for (size_t i = 0; i != _slaves.size(); ++i) {
				_slaves[i] = read<address_offset>(SLAVES_BEGIN + i * sizeof(address_offset));
			}
			return true;
		}

		virtual bool dump() {
			if (_slaves.size() > SLAVES_CAPACITY) {
				return false;
				// throw std::runtime_error("[DriveMasterPage::dump]");
			}
			write(_forward, FORWORD_POS);
			write(_back, BACK_POS);
			// partial dump
			if (size() == HEADER_SIZE) {
				return true;
			}

			write(static_cast<page_address>(_slaves.size()), SLAVES_SIZE_POS);
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

	template<drive_address ExpandSize = EXPAND_SIZE, drive_address ShrinkSize = SHRINK_SIZE>
	struct DriveFreeManager : BasicCacheHandler<drive_address, Page> {
		using address_offset = typename DriveMasterPage::address_offset;
		using cache_type = Cache<drive_address, Page, DriveFreeManager>;

		DriveBuffer &_buffer;
		std::vector<drive_address> &_masters;
		cache_type _cache;

		inline DriveFreeManager(DriveBuffer &buffer, std::vector<drive_address> &masters) : _buffer(buffer), _masters(masters), _cache(*this) {
		}

		inline ~DriveFreeManager() {
			close();
		}
		
		inline bool isOpen() {
			return _cache.isOpen();
		}

		inline void open(size_t capacity) {
			_cache.open(capacity);
			/*for (auto addr : _masters) {
				_cache.fetch<DriveMasterPage>(addr);
			}
			if (!_masters.empty()) {
				_cache.pin(_masters.back());
			}*/
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
				_cache.fetch<DriveMasterPage>(addr);
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
			_masters.resize(_cache._indexes.size());
			size_t j = 0;
			for (auto &i : _cache._indexes) {
				_masters[j++] = i.first;
			}
			std::sort(_masters.begin(), _masters.end());
			if (!unpin && !_masters.empty()) {
				_cache.pin(_masters.back());
			}
		}

		bool onHit(drive_address addr, Page &page) {
			// TODO: don't know how to get length from onHit parameters
			if (page.size() < PAGE_SIZE) {
				page.activate(page.begin(), page.begin() + PAGE_SIZE);
				_buffer.get(page, addr);
			}
			return true;
		}
		// handle Cache insert and put insert mapping value in &value
		bool onInsert(drive_address addr, Page &page) {
			return _buffer.get(page, addr);
		}
		// handle Cache erase and put mapping value in &value for cleaning/write_back ...
		bool onErase(drive_address addr, Page &page) {
			return page.size() != PAGE_SIZE || _buffer.put(page, addr);
		}

		void insert(drive_address addr) {
			// find start master page and traversing bound
			// pos is set to FIXED_DRIVE_ENTRY_PAGE when it might insert at the end
			drive_address left = 0, right = 0, pos = 0;
			if (!_masters.empty()) {
				auto iter = std::lower_bound(_masters.begin(), _masters.end(), addr);
				if (iter == _masters.end()) {
					right = *(iter - 1);
					pos = FIXED_DRIVE_ENTRY_PAGE;
					left = right;
				} else {
					right = *iter;
					pos = right;
					if (iter == _masters.begin()) {
						left = FIXED_DRIVE_ENTRY_PAGE;
					} else {
						left = *(iter - 1);
					}
				}
			}

			// traverse part of chain
			drive_address next = FIXED_DRIVE_ENTRY_PAGE;
			while (right != FIXED_DRIVE_ENTRY_PAGE) {
				if (right == addr) {
					throw std::runtime_error("[Drive::insert]");
				}
				auto offset = static_cast<address_offset>(addr / PAGE_SIZE - right / PAGE_SIZE);
				if (offset * PAGE_SIZE + right == addr) { // check if distance fits in address_offset
					auto ptr = _cache.fetch<DriveMasterPage>(right);
					if (ptr->_slaves.size() < DriveMasterPage::SLAVES_CAPACITY) { // TOOD: shit code, need resharp
						// auto sptr = cache.fetch<DriveSlavePage>(addr);
						// put(slave, addr);
						insertSlave(addr, right, *ptr);
						if (next != FIXED_DRIVE_ENTRY_PAGE) { // cache has been fetched so we should sync with masters for the next search
							dump();
						}
						return;
					} else {
						next = ptr->_forward;
					}
				} else {
					if (right < addr) {
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
			Container c(PAGE_SIZE);
			DriveMasterPage p(c, 0, PAGE_SIZE);
			if (pos == FIXED_DRIVE_ENTRY_PAGE) {
				if (!_masters.empty()) {
					auto fwd = _masters.back();
					p._forward = fwd;
					if (fwd != FIXED_DRIVE_ENTRY_PAGE) {
						auto ptr = _cache.fetch<DriveMasterPage>(fwd);
						ptr->_back = addr;
						_cache.unpin(fwd);
					}
				}
				_buffer.put(p, addr);
				_cache.fetch<DriveMasterPage>(addr);
				_cache.pin(addr);
			} else {
				auto ptr = _cache.fetch<DriveMasterPage>(pos);
				auto fwd = ptr->_forward;
				p._forward = fwd;
				p._back = pos;
				ptr->_forward = addr;
				if (fwd != FIXED_DRIVE_ENTRY_PAGE) {
					ptr = _cache.fetch<DriveMasterPage>(fwd);
					ptr->_back = addr;
				}
				_buffer.put(p, addr);
			}
		}

		inline void insertSlave(drive_address slave, drive_address master, DriveMasterPage &mpage) {
			auto offset = static_cast<address_offset>(slave / PAGE_SIZE - master / PAGE_SIZE);
			auto iter = std::lower_bound(mpage._slaves.begin(), mpage._slaves.end(), offset);
			mpage._slaves.insert(iter, offset);
		}

		// return the address actually erased (it is a private function for allocate), the address allocate just need to be close enough to the target
		// key point is to avoid I/O
		drive_address erase(drive_address addr, bool system = false) {
			drive_address result = 0;
			auto iter = std::lower_bound(_masters.begin(), _masters.end(), addr);
			if (iter == _masters.end()) {
				--iter;
			}
			auto ptr = _cache.fetch<DriveMasterPage>(*iter);
			if (!ptr->_slaves.empty()) {
				// TODO: static_cast overflow problem: easy use for random allocate now
				auto siter = std::lower_bound(ptr->_slaves.begin(), ptr->_slaves.end(), static_cast<address_offset>(addr / PAGE_SIZE - *iter / PAGE_SIZE));
				if (siter == ptr->_slaves.end()) {
					--siter;
				}
				result = *iter + static_cast<drive_address>(*siter) * PAGE_SIZE;
				ptr->_slaves.erase(siter);
				// eraseSlave(*ptr, result);

			} else {
				result = *iter;
				eraseMaster(*iter);
				dump();
			}

			return result;
		}

		inline void eraseMaster(drive_address addr) {
			auto ptr = _cache.fetch<DriveMasterPage>(addr);
			if (addr == _masters.back()) { // can use ptr->_back to check instead
				auto fwd = ptr->_forward;
				_cache.unpin(addr);
				_cache.discard(addr);
				if (fwd != FIXED_DRIVE_ENTRY_PAGE) {
					ptr = _cache.fetch<DriveMasterPage>(fwd);
					ptr->_back = FIXED_DRIVE_ENTRY_PAGE;
					_cache.pin(fwd);
				}
			} else {
				auto fwd = ptr->_forward, bck = ptr->_back;
				_cache.discard(addr);
				if (fwd != FIXED_DRIVE_ENTRY_PAGE) {
					ptr = _cache.fetch<DriveMasterPage>(fwd);
					ptr->_back = bck;
				}
				ptr = _cache.fetch<DriveMasterPage>(bck);
				ptr->_forward = fwd;
			}
		}

		inline void eraseSlave(DriveMasterPage &mpage, drive_address slave) { // TODO: verify
		}

		inline drive_address allocate(drive_address addr = 0) {
			if (_masters.empty()) {
				auto origin = _buffer.size();
				_buffer.expand(ExpandSize);
				auto current = _buffer.size();
				for (auto i = origin; i < current; i += PAGE_SIZE) {
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
		using cache_type = Cache<drive_address, Page, Drive>;

		constexpr static size_t ENTRY_PAGE_POS = BUFFER_PAGE_POS + PAGE_SIZE;
		constexpr static size_t CONTAINER_SIZE = ENTRY_PAGE_POS + PAGE_SIZE;

		DriveEntryPage _entry;
		DriveFreeManager<EXPAND_SYSTEM_SIZE, SHRINK_SYSTEM_SIZE> _system;
		DriveFreeManager<EXPAND_USER_SIZE, SHRINK_USER_SIZE> _user;

		inline Drive() : DriveBuffer(CONTAINER_SIZE), _entry(_container, FIXED_DRIVE_ENTRY_PAGE, FIXED_DRIVE_ENTRY_PAGE + PAGE_SIZE), _system(*this, _entry._systemMasters), _user(*this, _entry._userMasters) {
		}

		inline explicit Drive(const char * path, bool truncate = false) : Drive() {
			open(path, truncate);
		}

		inline explicit Drive(const std::string &path, bool truncate = false) : Drive(path.c_str(), truncate) {
		}

		inline ~Drive() {
			close();
		}
		// derive bool isOpen()

		inline void open(const char * path, bool truncate = false) {
			DriveBuffer::open(path, truncate);
			_entry.activate(ENTRY_PAGE_POS, ENTRY_PAGE_POS + PAGE_SIZE);
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
			_system.close();
			_user.close();
			dump();
			_entry.deactivate();
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
			_entry._freeSize = 0;
			for (auto i = FIXED_SIZE; i < FIXED_SIZE + INIT_SYSTEM_SIZE; i += PAGE_SIZE) {
				free(i, true);
			}
			for (auto i = FIXED_SIZE + INIT_SYSTEM_SIZE; i < INIT_SIZE; i += PAGE_SIZE) {
				free(i);
			}
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
