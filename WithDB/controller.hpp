#pragma once

#include "keeper.hpp"
#include "relation.hpp"
#include "relation_guard.hpp"

#include <iostream>
#include <string>
#include <sstream>

namespace db {
	struct MetadataGuard {
		constexpr static size_t BLOCK_CAPACITY = SEGMENT_SIZE / 4;
		constexpr static size_t RELATION_META_INDEX = 1;
		constexpr static size_t ATTRIBUTE_META_INDEX = 2;
		constexpr static size_t INDEX_META_INDEX = 3;

		Keeper &_keeper;
		std::unordered_map<std::string, Relation> _relations;
		RelationGuard _relationMeta;
		RelationGuard _attributeMeta;
		RelationGuard _indexMeta;

	public:
		inline MetadataGuard(Keeper &keeper) : _keeper(keeper), _relations{
			{"RelationMeta", Relation("RelationMeta") },
			{"AttributeMeta", Relation("AttributeMeta") },
			{"IndexMeta", Relation("IndexMeta") },
		}, _relationMeta(_keeper, _relations["RelationMeta"])
		, _attributeMeta(_keeper, _relations["AttributeMeta"])
		, _indexMeta(_keeper, _relations["IndexMeta"]) {
			// TODO: config
			{
				auto &relation = _relationMeta._relation;
				relation.add("Name", db::VARCHAR_T, 255)
					.add("Capacity", db::LONG_T)
					.add("Begin", db::LONG_T)
					.add("End", db::LONG_T)
					.add("Ptr", db::LONG_T)
					.add("FixedTupleSize", db::LONG_T)
					.add("MaxTupleSize", db::LONG_T)
					.add("TCount", db::LONG_T)
					.add("BCount", db::LONG_T)
					.format();
				relation._capacity = BLOCK_CAPACITY;
				relation._ptr = relation._begin = RELATION_META_INDEX * BLOCK_CAPACITY;
				auto p = _keeper.hold<VirtualPage>(0);
				relation._end = p->read<address>(0);
				relation._tCount = p->read<size_t>(sizeof(address));
				relation._bCount = p->read<size_t>(sizeof(address) + sizeof(size_t));
			}
			
			_relationMeta._relation._capacity = BLOCK_SIZE;
			_attributeMeta._relation
				.add("Name", db::VARCHAR_T, 255)
				.add("Type", db::INT_T)
				.add("Size", db::INT_T)
				.add("Index", db::INT_T)
				.add("Offset", db::INT_T)
				.add("VCount", db::INT_T)
				.format();
		}
	};

	struct Controller {
		Keeper _keeper;

		Controller(const char *path, bool truncate = false): _keeper(path, truncate) {
			k.start();
			std::cerr << std::hex;
		}

		void close() {
			k.stop();
			k.close();
		}

		~Controller() {
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
					throw std::runtime_error("[Controller::put] unknown type");
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
					throw std::runtime_error("[Controller::get] unknown type");
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