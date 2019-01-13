#pragma once

#include "controller.hpp"
#include "relation.hpp"
#include "query.hpp"

#include <typeinfo>

namespace db {
	struct QueryExecutor {
		std::unordered_map<size_t, size_t> _map;
		Controller &_controller;
		QueryPlan _plan;
		size_t _pos = 0;
		inline QueryExecutor(Controller &controller, QueryPlan &&plan)
			: _controller(controller), _plan(std::move(plan)) {
		}
		inline QueryExecutor(const QueryExecutor &other) = delete;
		inline QueryExecutor(QueryExecutor &&other) = default;

		inline ~QueryExecutor() {
			for (auto &p : _map) {
				_controller.dropTemp(p.second);
			}
		}
		inline QueryExecutor &operator=(const QueryExecutor &other) = delete;
		inline QueryExecutor &operator=(QueryExecutor &&other) = default;

		inline size_t excecute() {
			// execute 
			size_t ret = 0;
			for (auto p : _plan) {
				if (typeid(*p) == typeid(UnaryQueryStep)) {
					auto &step = *reinterpret_cast<UnaryQueryStep *>(p);
					if (getFlag(step._single, 63)) {
						auto iter = _map.find(step._single);
						if (iter == _map.end()) {
							throw std::runtime_error("[QueryExecutor::excecute]");
						} else {
							step._single = iter->second;
						}
					}
					ret = _controller.query(step);
				} else if (typeid(*p) == typeid(BinaryQueryStep)) {
					auto &step = *reinterpret_cast<BinaryQueryStep *>(p);
					if (getFlag(step._left, 63)) {
						auto iter = _map.find(step._left);
						if (iter == _map.end()) {
							throw std::runtime_error("[QueryExecutor::excecute]");
						} else {
							step._left = iter->second;
						}
					}
					if (getFlag(step._right, 63)) {
						auto iter = _map.find(step._right);
						if (iter == _map.end()) {
							throw std::runtime_error("[QueryExecutor::excecute]");
						} else {
							step._right = iter->second;
						}
					}
					ret = _controller.query(step);
				} else {
					throw std::runtime_error("[QueryExecutor::excecute]");
				}
				if (!_map.try_emplace(p->_tmp, ret).second) {
					throw std::runtime_error("[QueryExecutor::excecute]");
				}
			}
			return _pos = ret; // returned final result index
		}

		inline bool result() {
			if (!getFlag(_pos, 63)) {
				return false;
			}
			_controller.printResult(_pos);
			return true;
		}
	};

	struct QueryExcecutorFactory {
		Controller &_controller;

		inline QueryExcecutorFactory(Controller &controller) : _controller(controller) {
		}

		inline QueryExecutor getInstance(QueryPlan &&plan) {
			return QueryExecutor(_controller, std::move(plan));
		}
	};
}

