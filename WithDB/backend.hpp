#pragma once

#include "controller.hpp"
#include "executor.hpp"

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
	};
}