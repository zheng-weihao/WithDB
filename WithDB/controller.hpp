#pragma once

#include "keeper.hpp"
#include "tuple.hpp"

#include <iostream>
#include <string>

// simple controller to wrap, almost crap
namespace db {

	struct controller {
		std::shared_ptr<tuple_table> table;
		keeper k;

		controller(const char *path): k(path, false) {
			table = std::make_shared<db::tuple_table>(db::tuple_table{
				db::tuple_entry(db::INT_T),
				db::tuple_entry(db::CHAR_T, 18),
				db::tuple_entry(db::VARCHAR_T),
				db::tuple_entry(db::INT_T),
				db::tuple_entry(db::CHAR_T, 15),
				db::tuple_entry(db::DOUBLE_T),
				db::tuple_entry(db::VARCHAR_T),
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
			close(); // TODO: set flag is_open
		}

		address put(const std::string &row, address start = 0) {
			std::stringstream ss(row);
			std::string item;
			int i = 0;
			db::tuple_builder builder;
			builder.set_table(table);
			builder.start();
			while (std::getline(ss, item, '|')) {
				attribute_type_enum e = (*table)[i].get_type();
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
				db::tuple_page p(std::move(k.hold(addr)));
				try {
					p.load();
				} catch (std::out_of_range e) {
					p.init();
				}
				try {
					auto result = p.allocate(static_cast<page_address>(out->size()));
					auto pa = p.get(result);
					p.copy_from(out->begin(), pa.first, pa.second);
					ret = p.addr + result;
					break;
				} catch (std::out_of_range e) {
					// std::cerr << e.what() << endl;
				}
			}
			return ret;
		}

		void put_from_file(const std::string &filename) {
			std::fstream fs(filename);
			std::string row;
			address result = 0;
			while (std::getline(fs, row)) {
				result = put(row, (result / PAGE_SIZE) * PAGE_SIZE);
				std::cerr << "put log in address " << result << std::endl;
			}
		}

		std::string get(address addr) {
			db::tuple_page p(std::move(k.hold((addr / PAGE_SIZE) * PAGE_SIZE)));
			std::stringstream ss;
			try {
				p.load();
			} catch (std::out_of_range e) {
				return ss.str();
			}
			auto pa = p.get(static_cast<page_address>(addr % PAGE_SIZE));
			if (pa.second == 0) {
				return ss.str();
			}
			db::tuple tmp(pa.second - pa.first);
			p.copy_to(tmp.begin(), pa.first, pa.second);
			for (int i = 0; i < table->size(); ++i) {
				attribute_type_enum e = (*table)[i].get_type();
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
				tuple_page p(std::move(k.hold(addr)));
				try {
					p.load();
				} catch (std::out_of_range e) {
					break;
				}
				
				for (auto &entry : p.piece_table) {
					std::cout << get(p.addr + entry.index) << std::endl;
					++counter;
					if (br && counter % br == 0) {
						char ch;
						std::getchar();
					}
				}
			}
			return counter;
		}
	};
}