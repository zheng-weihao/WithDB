#pragma once

#include "controller.hpp"
#include "executor.hpp"

#include <fstream>
#include <sstream>

namespace db {
	struct Backend {
		Controller _controller;
		// Parser
		//QueryOptimizer _optimizer;
		QueryExcecutorFactory _factory;

		Backend(const string &path, bool truncate = false)
			: _controller(path, truncate)//, _optimizer(_controller._metaGuard._schema)
			, _factory(_controller) {
		}

		bool workflow(const string &statement = "") {
			// Parser
			// if optimizer :
			//
			// else::
			// controller.someMethod
			db::Relation table(string("foo"));
			table.addAttribute("int", db::INT_T)
				.addAttribute("varchar", db::VARCHAR_T, 100)
				.format();
			auto tmp = table;
			_controller.createRelation(std::move(table));
			auto builder = _controller.relationGuard("foo")._relation.builder();
			for (auto i = 0; i < 0x20; ++i) {
				db::Tuple tmp = builder.start()
					.build(0, i)
					.build(1, string("Supplier:") + toString(i))
					.complete();
				_controller.createTuple("foo", tmp);
			}
			auto p = new UnaryQueryStep(std::move(Relation().addAttribute("varchar", db::VARCHAR_T, 100).format())
				, _controller.schema().relationPos("foo"));
			p->_selection = [](Tuple &tuple) { return tuple.get<int>(0) < 5; };
			p->_projection.resize(_controller.schema().relation("foo")._attributes.size(), -1);
			p->_projection[1] = 0;
			QueryPlan plan;
			plan.push_back(p);
			{
				auto excecutor = _factory.getInstance(std::move(plan));
				excecutor.excecute();
				excecutor.result();
			}
			
			std::cout << "print relation: RelationMeta" << std::endl;
			_controller.printAll("RelationMeta");
			std::cout << "print relation: AttributeMeta" << std::endl;
			_controller.printAll("AttributeMeta");
			std::cout << "print relation: IndexMeta" << std::endl;
			_controller.printAll("IndexMeta");
			std::cout << "print relation: foo" << std::endl;
			_controller.printAll("foo");
			return true;
		}

		template<typename Key>
		bool loadFile(const string &path, const Key &relation, bool log = false) {
			std::fstream fs(path);
			string row;
			size_t success = 0;
			size_t total = 0;
			auto builder = _controller.relation(relation).builder();
			while (std::getline(fs, row)) {
				try {
					std::stringstream ss(row);
					size_t i = 0;
					string value;
					builder.start();
					while (std::getline(ss, value, '|')) {
						builder.build(i, value);
					}
					auto tuple = builder.complete();
					auto ret = _controller.createTuple(relation, tuple);
					if (log) {
						std::cerr << "put tuple " << total <<" in address " << ret << std::endl;
					}
					++success;
				} catch (const std::runtime_error &e) {
					e.what();
				}
				++total;
			}
			if (log) {
				std::cerr << "load file success in " << relation << ", statistic = " << success << "/" << total << std::endl;
			}
			return success == total;
		}

		template<typename Key>
		bool dumpFile(const string &path, const Key &relation, bool log = false) {
			std::fstream fs(path, std::ios::out);
			string row;
			size_t success = 0;
			size_t total = 0;
			_controller.relationGuard(key).traverseTuple([&fs, &success, &total](Tuple &tuple, address) {
				auto size = tuple._relation.attributeSize()
				for (size_t i = 0; i != size); ++i) {
					fs << tuple.get<string>(i);
					if (i == size - 1) {
						fs << std::endl;
					} else {
						fs << '|';
					}
				}
				++success;
				++total;
			});
			if (log) {
				std::cerr << "dump file success with " << relation << ", statistic = " << success << "/" << total << std::endl;
			}
			return success == total;
		}
	};
}