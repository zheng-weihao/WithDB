#pragma once

#include "keeper.hpp"
#include "tuple.hpp"

#include <iostream>
#include <string>
#include <sstream>

// simple controller to wrap, almost crap
namespace db {

	struct controller {
		std::shared_ptr<AttributeTable> table;
		keeper k;

		controller(const char *path, bool trunc = false): k(path, trunc) {
			table = std::make_shared<db::AttributeTable>(db::AttributeTable{
				db::AttributeEntry(db::INT_T),
				db::AttributeEntry(db::CHAR_T, 18),
				db::AttributeEntry(db::VARCHAR_T),
				db::AttributeEntry(db::INT_T),
				db::AttributeEntry(db::CHAR_T, 15),
				db::AttributeEntry(db::DOUBLE_T),
				db::AttributeEntry(db::VARCHAR_T),
				}
			);
			k.start();
			std::cerr << std::hex;
		}

		void close() {
			k.stop();
			k.close();
		}

		~controller() {
			close(); // TODO: set flag isOpen
		}

		address put(const std::string &row, address start = 0) {
			std::stringstream ss(row);
			std::string item;
			int i = 0;
			db::TupleBuilder builder;
			builder.set_table(table);
			builder.start();
			while (std::getline(ss, item, '|')) {
				attribute_enum e = (*table)[i].get_type();
				switch (e) {
				case db::CHAR_T:
				case db::VARCHAR_T:
				case db::DATE_T:
					builder.set(item, i);
					break;
				case db::INT_T:
					builder.set(std::stoi(item), i);
					break;
				case db::LONG_T:
					builder.set(std::stoll(item), i);
					break;
				case db::FLOAT_T:
					builder.set(std::stof(item), i);
					break;
				case db::DOUBLE_T:
					builder.set(std::stod(item), i);
					break;
				case db::BLOB_T:
				default:
					throw std::runtime_error("[controller::put] unknown type");
				}
				++i;
			}

			auto out = builder.get();
			builder.reset();
			address ret = 0;

			for (db::address addr = start; addr < SEGMENT_SIZE; addr += db::PAGE_SIZE) {
				db::TuplePage p(std::move(k.hold(addr)));
				try {
					p.load();
				} catch (std::runtime_error e) {
					p.init();
				}
				try {
					auto result = p.allocate(static_cast<page_address>(out->size()));
					auto pa = p.get(result);
					p.copy_from(out->begin(), pa.first, pa.second);
					ret = p.addr + result;
					break;
				} catch (std::runtime_error e) {
					// std::cerr << e.what() << endl;
				}
			}
			return ret;
		}

		void put_from_file(const std::string &filename, bool all_log = true) {
			std::fstream fs(filename);
			std::string row;
			int counter = 0;
			address result = 0;
			while (std::getline(fs, row)) {
				result = put(row, (result / PAGE_SIZE) * PAGE_SIZE);
				if (all_log) {
					std::cerr << "put log in address " << result << std::endl;
				}
				++counter;
			}
			std::cerr << "put success, total = " << counter << std::endl;
		}

		std::string get(address addr) {
			db::TuplePage p(std::move(k.hold((addr / PAGE_SIZE) * PAGE_SIZE)));
			std::stringstream ss;
			try {
				p.load();
			} catch (std::runtime_error e) {
				return ss.str();
			}
			auto pa = p.get(static_cast<page_address>(addr % PAGE_SIZE));
			if (pa.second == 0) {
				return ss.str();
			}
			db::tuple tmp(pa.second - pa.first);
			p.copy_to(tmp.begin(), pa.first, pa.second);
			for (int i = 0; i < table->size(); ++i) {
				attribute_enum e = (*table)[i].get_type();
				switch (e) {
				case db::CHAR_T:
				case db::VARCHAR_T:
				case db::DATE_T:
					ss << table->get<std::string>(tmp, i);
					break;
				case db::INT_T:
					ss << table->get<int_type>(tmp, i);
					break;
				case db::LONG_T:
					ss << table->get<long_type>(tmp, i);
					break;
				case db::FLOAT_T:
					ss << table->get<float_type>(tmp, i);
					break;
				case db::DOUBLE_T:
					ss << table->get<double_type>(tmp, i);
					break;
				case db::BLOB_T:
				default:
					throw std::runtime_error("[controller::get] unknown type");
				}
				ss << '\t';
			}
			return ss.str();
		}

		int get_all(int br = 0, address start = 0) {
			int counter = 0;
			for (auto addr = start; addr >= 0; addr += PAGE_SIZE) {
				TuplePage p(std::move(k.hold(addr)));
				try {
					p.load();
				} catch (std::runtime_error e) {
					break;
				}
				
				for (auto &entry : p._entries) {
					std::cout << get(p.addr + entry._index) << std::endl;
					++counter;
					if (br && counter % br == 0) {
						std::getchar();
					}
				}
			}
			return counter;
		}
	};
}