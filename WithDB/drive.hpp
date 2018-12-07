#ifndef __DRIVE_HPP__
#define __DRIVE_HPP__

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
	struct fpage_wrapper {
		constexpr static std::ios_base::openmode DEFAULT_MODE = std::ios_base::in | std::ios_base::out;
		std::filesystem::path path;
		std::fstream fs;

	public:
		fpage_wrapper() {
		}

		explicit fpage_wrapper(const char * filename, std::ios_base::openmode mode = DEFAULT_MODE) {
			open(filename, mode);
		}

		explicit fpage_wrapper(const std::string &filename, std::ios_base::openmode mode = DEFAULT_MODE) {
			open(filename, mode);
		}

		void open(const char *filename, std::ios_base::openmode mode = DEFAULT_MODE) {
			// check if the file exists
			path = filename;
			fs.open(path, std::ios_base::in);
			bool exists = fs.is_open();
			if (!exists) {
				fs.open(path, std::ios_base::out);
			}
			fs.close();

			mode |= std::ios_base::binary;
			fs.open(path, mode);
			fs.unsetf(std::ios_base::skipws); // important trap
		}

		void open(const std::string &filename, std::ios_base::openmode mode = DEFAULT_MODE) {
			open(filename.c_str(), mode);
		}

		bool is_open() const {
			return fs.is_open();
		}

		void close() {
			fs.close();
		}

		drive_address size() {
			return std::filesystem::file_size(path);
		}

		void expand(drive_address size = EXPAND_SIZE) {
			std::filesystem::resize_file(path, this->size() + size);
		}

		void shrink(drive_address size = SHRINK_SIZE) {
			std::filesystem::resize_file(path, this->size() - size);
		}

		fpage_wrapper &get(page &p, drive_address addr, bool load = true, bool sync = true) {
			if (addr % PAGE_SIZE) {
				throw std::runtime_error("[fpage_wrapper::get] physical address doesn't align to page size");
			}

			if (sync) {
				fs.sync(); // TODO: find better sync time
			}

			if (fs.tellg() != addr) {
				fs.seekg(addr);
			}
			
			std::istream_iterator<char> in(fs);
			std::copy_n(in, p.size(), p.begin());
			// hack way to read to continuous memory container
			// fs.read(&*p.begin(), p.size());

			if (load) {
				p.load();
			}

			return *this;
		}

		fpage_wrapper &put(page &p, drive_address addr, bool dump = true, bool flush = true) {
			if (addr % PAGE_SIZE) {
				throw std::runtime_error("[fpage_wrapper::put] physical address doesn't align to page size");
			}

			if (dump) {
				p.dump();
			}

			if (fs.tellp() != addr) {
				fs.seekp(addr);
			}
			
			std::ostream_iterator<char> out(fs);
			std::copy(p.begin(), p.end(), out);
			
			if (flush) {
				fs.flush(); // TODO: find better flush time
			}

			return *this;
		}
	};

	// TODO: better name design and replace saving vectors' end address with size for better comprehension
	// names are ugly
	struct io_entry_page : page {
		constexpr static page_address TOTAL_SIZE_POS = 0;
		constexpr static page_address FREE_SIZE_POS = 8;
		constexpr static page_address SYSTEM_FREE_MASTER_PTRS_END_POS = 1020;
		constexpr static page_address USER_FREE_MASTER_PTRS_END_POS = 1022;

		constexpr static page_address SYSTEM_FREE_MASTER_PTRS_BEGIN = 1024;
		constexpr static page_address SYSTEM_FREE_MASTER_PTRS_END = 2048;
		constexpr static page_address SYSTEM_FREE_MASTER_PTRS_SIZE = (SYSTEM_FREE_MASTER_PTRS_END - SYSTEM_FREE_MASTER_PTRS_BEGIN) / static_cast<page_address>(sizeof(drive_address));

		constexpr static page_address USER_FREE_MASTER_PTRS_BEGIN = 2048;
		constexpr static page_address USER_FREE_MASTER_PTRS_END = 4096;
		constexpr static page_address USER_FREE_MASTER_PTRS_SIZE = (USER_FREE_MASTER_PTRS_END - USER_FREE_MASTER_PTRS_BEGIN) / static_cast<page_address>(sizeof(drive_address));

		drive_address total_size; // [0,8)
		drive_address free_size; // [8,16)

		// TODO: other timestamp value, like last sync at, ...
		// timestamp syncAt;

		// page_address system_free_master_ptrs_end [1020, 1022)
		// page_address user_free_master_ptrs_end [1022, 1024)
		std::vector<drive_address> system_free_master_ptrs; // [1024, 2048)
		std::vector<drive_address> user_free_master_ptrs; // [2048, 4096)

	public:
		inline io_entry_page(iterator first, iterator last) : basic_page(first, last) {
		}

		virtual void load() {
			total_size = read<drive_address>(TOTAL_SIZE_POS);
			free_size = read<drive_address>(FREE_SIZE_POS);


			auto system_free_master_ptrs_end = read<page_address>(SYSTEM_FREE_MASTER_PTRS_END_POS);
			auto user_free_master_ptrs_end = read<page_address>(USER_FREE_MASTER_PTRS_END_POS);

			system_free_master_ptrs.clear();
			user_free_master_ptrs.clear();
			for (page_address i = SYSTEM_FREE_MASTER_PTRS_BEGIN; i != system_free_master_ptrs_end; i += sizeof(drive_address)) {
				system_free_master_ptrs.push_back(read<drive_address>(i));
			}
			for (page_address i = USER_FREE_MASTER_PTRS_BEGIN; i != user_free_master_ptrs_end; i += sizeof(drive_address)) {
				user_free_master_ptrs.push_back(read<drive_address>(i));
			}
		}

		virtual void dump() {
			write(total_size, TOTAL_SIZE_POS);
			write(free_size, FREE_SIZE_POS);

			auto system_free_master_ptrs_end = SYSTEM_FREE_MASTER_PTRS_END_POS;
			auto user_free_master_ptrs_end = USER_FREE_MASTER_PTRS_END_POS;

			if (system_free_master_ptrs.size() > SYSTEM_FREE_MASTER_PTRS_SIZE || user_free_master_ptrs.size() > USER_FREE_MASTER_PTRS_SIZE) {
				throw std::out_of_range("[io_entry_page::update] free_master_ptrs are out of range");
			}

			write(static_cast<page_address>(system_free_master_ptrs.size() * sizeof(drive_address) + SYSTEM_FREE_MASTER_PTRS_BEGIN), SYSTEM_FREE_MASTER_PTRS_END_POS);
			write(static_cast<page_address>(user_free_master_ptrs.size() * sizeof(drive_address) + USER_FREE_MASTER_PTRS_BEGIN), USER_FREE_MASTER_PTRS_END_POS);

			page_address i = SYSTEM_FREE_MASTER_PTRS_BEGIN;
			for (auto ptr : system_free_master_ptrs) {
				write(ptr, i);
				i += sizeof(drive_address);
			}
			i = USER_FREE_MASTER_PTRS_BEGIN;
			for (auto ptr : user_free_master_ptrs) {
				write(ptr, i);
				i += sizeof(drive_address);
			}
		}
	};

	// TODO: use int<24> to replace offset ptr value type for better perform
	struct free_master_page : page {
		constexpr static page_address FORWORD_PTR_POS = 0;
		constexpr static page_address BACK_PTR_POS = 8;
		constexpr static page_address HEADER_SIZE = 16;
		constexpr static page_address FREE_SLAVE_OFFSETS_END_POS = 16;
		constexpr static page_address FREE_SLAVE_OFFSETS_BEGIN = 18;
		constexpr static page_address FREE_SLAVE_OFFSETS_END = 4096;
		constexpr static page_address FREE_SLAVE_OFFSETS_SIZE = (FREE_SLAVE_OFFSETS_END - FREE_SLAVE_OFFSETS_BEGIN) / static_cast<page_address>(sizeof(free_page_offset));

		drive_address forward_ptr; // [0, 8)
		drive_address back_ptr; // [8, 16)
		// page_address free_slave_offsets_end [16, 18)

		std::vector<free_page_offset> free_slave_offsets; // [18, 4096)

		inline free_master_page(iterator first, iterator last) : basic_page(first, last) {
		}

		virtual void load() {
			forward_ptr = read<drive_address>(FORWORD_PTR_POS);
			back_ptr = read<drive_address>(BACK_PTR_POS);

			// allow partial load
			if (size() <= HEADER_SIZE) {
				return;
			}

			free_slave_offsets.clear();
			auto free_slave_offsets_end = read<page_address>(FREE_SLAVE_OFFSETS_END_POS);
			for (page_address i = FREE_SLAVE_OFFSETS_BEGIN; i != free_slave_offsets_end; i += sizeof(free_page_offset)) {
				free_slave_offsets.push_back(read<free_page_offset>(i));
			}
		}

		virtual void dump() {
			write(forward_ptr, FORWORD_PTR_POS);
			write(back_ptr, BACK_PTR_POS);

			// allow partial dump
			if (size() <= HEADER_SIZE) {
				return;
			}


			if (free_slave_offsets.size() > FREE_SLAVE_OFFSETS_SIZE) {
				throw std::out_of_range("[free_master_page::dump] free_slave_offsets are out of range");
			}
			
			write(static_cast<page_address>(free_slave_offsets.size() * sizeof(free_page_offset) + FREE_SLAVE_OFFSETS_BEGIN), FREE_SLAVE_OFFSETS_END_POS);

			page_address i = FREE_SLAVE_OFFSETS_BEGIN;
			for (auto ptr : free_slave_offsets) {
				write(ptr, i);
				i += sizeof(free_page_offset);
			}
		}
	};

	struct free_slave_page : page {
		constexpr static page_address MASTER_PTR_POS = 0;
		constexpr static page_address HEADER_SIZE = 8;

		drive_address master_ptr;
		inline free_slave_page(iterator first, iterator last) : basic_page(first, last) {
		}

		virtual void load() {
			master_ptr = read<drive_address>(MASTER_PTR_POS);
		}

		virtual void dump() {
			write(master_ptr, MASTER_PTR_POS);
		}
	};

	// sync controller for database io management
	struct drive : fpage_wrapper {
		std::vector<char> entry_memory;

		io_entry_page entry;
		// TODO: consider if using cache for master pages is necessary
		std::vector<char> master_memory;
		std::vector<char> slave_memory;
		free_master_page master, tmp_master; // full load master and header load tmp_master
		free_slave_page slave;
		std::mt19937 random_engine;

		drive() :
			entry_memory(PAGE_SIZE), entry(entry_memory.begin(), entry_memory.end()),
			master_memory(PAGE_SIZE + free_master_page::HEADER_SIZE), master(master_memory.begin(), master_memory.begin() + PAGE_SIZE), tmp_master(master_memory.begin() + PAGE_SIZE, master_memory.end()),
			slave_memory(free_slave_page::HEADER_SIZE), slave(slave_memory.begin(), slave_memory.end()) {
			std::random_device rd;
			random_engine = std::mt19937(rd());
		}

		explicit drive(const char * filename, bool trunc = false) : drive() {
			open(filename, trunc);
		}

		explicit drive(const std::string &filename, bool trunc = false) : drive(filename.c_str(), trunc) {
		}

		void open(const char * filename, bool trunc = false) {
			if (trunc) {
				std::filesystem::remove(filename);
			}
			fpage_wrapper::open(filename);
			if (size()) {
				load();
			} else {
				init();
			}
		}

		void open(const std::string &filename) {
			open(filename.c_str());
		}

		void close() {
			save();
			fpage_wrapper::close();
		}

		void insert_master(std::vector<drive_address> &mptrs, page_address limit, drive_address addr) {
			if (addr == 0x0000000000062000) {
				addr += 1;
				addr -= 1;
			}
			// find forward free master
			drive_address prev = 0, next = 0;
			auto iter = std::lower_bound(mptrs.begin(), mptrs.end(), addr);
			if (!mptrs.empty()) {
				if (iter != mptrs.end() && *iter == addr) {
					throw std::runtime_error("[drive::insert_master] duplication in master_ptrs");
				}
				if (iter == mptrs.end()) {
					prev = *(iter - 1);
				} else {
					prev = *iter;
					while (prev > *iter) {
						get(tmp_master, prev);
						prev = tmp_master.forward_ptr;
					}
				}
				if (!prev) {
					next = mptrs[0];
				}
			}
			if (prev == addr) {
				throw std::runtime_error("[drive::insert_master] duplication in master_ptrs");
			}
			if (prev) {
				get(tmp_master, prev);
				next = tmp_master.back_ptr;
				tmp_master.back_ptr = addr;
				put(tmp_master, prev);
			}

			if (next) {
				get(tmp_master, next);
				tmp_master.forward_ptr = addr;
				put(tmp_master, next);
			}

			master.forward_ptr = prev;
			master.back_ptr = next;
			
			mptrs.insert(iter, addr);
			while (mptrs.size() > limit) {
				std::uniform_int_distribution<> dis(1, static_cast<int>(size()) - 2);
				mptrs.erase(mptrs.begin() + dis(random_engine));
			}
		}

		void erase_master(std::vector<drive_address> &mptrs, page_address limit, drive_address addr) {
			if (addr == 0x0000000000062000) {
				addr += 1;
				addr -= 1;
			}
			entry.free_size -= PAGE_SIZE;
			auto prev = master.forward_ptr, next = master.back_ptr;
			if (prev) {
				get(tmp_master, prev);
				tmp_master.back_ptr = next;
				put(tmp_master, prev);
			}

			if (next) {
				get(tmp_master, next);
				tmp_master.forward_ptr = prev;
				put(tmp_master, next);
			}

			// TODO: improve performance
			auto iter = std::lower_bound(mptrs.begin(), mptrs.end(), addr);
			if (iter != mptrs.end() && *iter == addr) {
				mptrs.erase(iter);
				if (prev) {
					auto iter = std::lower_bound(mptrs.begin(), mptrs.end(), prev);
					if (iter == mptrs.end() || *iter != prev) {
						mptrs.insert(iter, prev);
					}
				}
				if (next) {
					auto iter = std::lower_bound(mptrs.begin(), mptrs.end(), next);
					if (iter == mptrs.end() || *iter != next) {
						mptrs.insert(iter, next);
					}
				}

				while (mptrs.size() > limit) {
					std::uniform_int_distribution<> dis(1, static_cast<int>(size()) - 2);
					mptrs.erase(mptrs.begin() + dis(random_engine));
				}
			}
		}

		void insert_slave(drive_address addr, drive_address slave_addr) {
			if (addr == 0x0000000000062000) {
				addr += 1;
				addr -= 1;
			}
			auto offset = static_cast<free_page_offset>(slave_addr / PAGE_SIZE) - static_cast<free_page_offset>(addr / PAGE_SIZE);
			auto iter = std::lower_bound(master.free_slave_offsets.begin(), master.free_slave_offsets.end(), offset);
			master.free_slave_offsets.insert(iter, offset);
			slave.master_ptr = addr;
		}

		void erase_slave(drive_address addr, drive_address slave_addr) {
			if (addr == 0x0000000000062000) {
				addr += 1;
				addr -= 1;
			}
			entry.free_size -= PAGE_SIZE;
			auto offset = static_cast<free_page_offset>(slave_addr / PAGE_SIZE) - static_cast<free_page_offset>(addr / PAGE_SIZE);
			auto iter = std::lower_bound(master.free_slave_offsets.begin(), master.free_slave_offsets.end(), offset);
			if (iter != master.free_slave_offsets.end() && *iter == offset) {
				master.free_slave_offsets.erase(iter);
			}
		}

		void insert_free(drive_address addr, bool system) {
			if (addr == 0x0000000000062000) {
				addr += 1;
				addr -= 1;
			}
			entry.free_size += PAGE_SIZE;
			auto &mptrs = system ? entry.system_free_master_ptrs : entry.user_free_master_ptrs;
			// find start master page and traversing bound
			drive_address tmp = 0, bound = 0;
			if (!mptrs.empty()) {
				auto iter = std::lower_bound(mptrs.begin(), mptrs.end(), addr);
				if (iter == mptrs.end()) {
					tmp = *(iter - 1);
					bound = tmp;
				} else {
					tmp = *iter;
					if (iter == mptrs.begin()) {
						bound = tmp;
					} else {
						bound = *(iter - 1);
					}
				}
			}
			
			// traverse part of chain
			if (tmp) {
				while(true) {
					auto cast_value = addr / PAGE_SIZE - tmp / PAGE_SIZE;
					auto offset = static_cast<free_page_offset>(cast_value);
					drive_address next = 0;
					if (static_cast<drive_address>(offset) == cast_value) {
						get(master, tmp);
						next = master.forward_ptr;
						if (master.free_slave_offsets.size() < free_master_page::FREE_SLAVE_OFFSETS_SIZE) {
							get(slave, addr);
							insert_slave(tmp, addr);
							put(slave, addr);
							put(master, tmp);
							return;
						}
					} else {
						get(tmp_master, tmp);
						next = tmp_master.forward_ptr;
					}
					if (tmp == bound) {
						break;
					}
					tmp = next;
				}
			}
			auto limit = system ? io_entry_page::SYSTEM_FREE_MASTER_PTRS_SIZE : io_entry_page::USER_FREE_MASTER_PTRS_SIZE;
			master.free_slave_offsets.clear();
			get(master, addr, false);
			insert_master(mptrs, limit, addr);
			put(master, addr);
		}

		void init() {
			expand(INIT_SIZE);
			get(entry, FIXED_IO_ENTRY_PAGE, false);
			entry.total_size = INIT_SIZE;
			entry.free_size = 0;

			for (auto i = FIXED_SIZE; i < FIXED_SIZE + INIT_SYSTEM_SIZE; i += PAGE_SIZE) {
				insert_free(i, true);
			}

			for (auto i = FIXED_SIZE + INIT_SYSTEM_SIZE; i < INIT_SIZE; i += PAGE_SIZE) {
				insert_free(i, false);
			}
			put(entry, FIXED_IO_ENTRY_PAGE);
		}

		void load() {
			get(entry, 0);
		}

		void save() {
			put(entry, 0);
		}

		drive_address allocate(drive_address index = 0, bool system = false) {
			auto &mptrs = system ? entry.system_free_master_ptrs : entry.user_free_master_ptrs;
			if (mptrs.empty()) {
				auto origin = size();
				expand(EXPAND_SIZE);
				auto current = size();
				for (auto i = origin; i < current; i += PAGE_SIZE) {
					insert_free(i, system);
				}
			}

			auto iter = std::lower_bound(mptrs.begin(), mptrs.end(), index);
			if (iter == mptrs.end()) {
				--iter;
			}
			get(master, *iter);
			if (!master.free_slave_offsets.empty()) {
				// TODO: static_cast overflow problem: easy use for random allocate now
				auto siter = std::lower_bound(master.free_slave_offsets.begin(), master.free_slave_offsets.end(), static_cast<free_page_offset>(index / PAGE_SIZE - *iter / PAGE_SIZE));
				if (siter == master.free_slave_offsets.end()) {
					--siter;
				}
				auto result = *iter + static_cast<drive_address>(*siter) * PAGE_SIZE;
				erase_slave(*iter, result);
				put(master, *iter);
				return result;
			} else {
				auto limit = system ? io_entry_page::SYSTEM_FREE_MASTER_PTRS_SIZE : io_entry_page::USER_FREE_MASTER_PTRS_SIZE;
				auto result = *iter;
				erase_master(mptrs, limit, *iter);
				return result;
			}

		}

		void free(drive_address addr, bool system = false) {
			insert_free(addr, system);
		}

	};
}

#endif // __DRIVE_HPP__
