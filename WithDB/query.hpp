#pragma once

#include "definitions.hpp"
#include "relation.hpp"
#include "utils.hpp"

namespace db {
	struct VariableEntry {
		type_enum _type;
		size_t _offset;

		VariableEntry(type_enum type = DUMMY_T, size_t offset = 0) : _type(type), _offset(offset) {
		}
	};

	struct ExprBase {
		type_enum _type;
	};

	struct ConditionExpr : ExprBase {

	};

	struct ComparisonExpr : ConditionExpr {

	};

	struct BooleanExpr : ConditionExpr {

	};

	struct RelationsExpr : ExprBase {

	};

	struct AttributesExpr : ExprBase {

	};

	struct TupleExpr : ExprBase {
		Tuple _tuple;
	};

	struct FunctionExpr : ExprBase { // operation with return value

	};

	struct AssignmentExpr : ExprBase {

	};

	struct CreateTableExpr : ExprBase {
		Relation _relation;
	};

	struct CreateIndexExpr : ExprBase {

	};

	struct InsertExpr : ExprBase {

	};

	struct SelectExpr : ExprBase {

	};

	struct UpdateExpr : ExprBase {

	};

	struct DeleteExpr : ExprBase {

	};

	struct Query {
		std::vector<element_t> _storage;
		std::vector<VariableEntry> _variables;
		std::vector<ExprBase *> _exprs;
		size_t _root;
	};

	struct QueryStep {
		Relation _result;
		size_t _tmp;

		QueryStep(Relation &&result, size_t tmp = -1) : _result(std::move(result)), _tmp(tmp) {
		}

		virtual string dummy() { return "QueryStep"; }
	};

	struct UnaryQueryStep : QueryStep {
		size_t _single;
		std::function<bool(Tuple &)> _selection;
		std::vector<size_t> _projection;
		UnaryQueryStep(Relation &&result, size_t single, size_t tmp = -1) 
			: QueryStep(std::move(result), tmp), _single(single) {
			_selection = [](Tuple &) { return true; };
		}

		virtual string dummy() { return "UnaryQueryStep"; }
	};

	struct BinaryQueryStep : QueryStep {
		size_t _left, _right;
		std::function<bool(Tuple &, Tuple &)> _join;
		std::vector<size_t> _projection;
		BinaryQueryStep(Relation &&result, size_t left, size_t right, size_t tmp = -1)
			: QueryStep(std::move(result), tmp), _left(left), _right(right) {
			_join = [](Tuple &, Tuple &) { return true; };
		}

		virtual string dummy() { return "BinaryQueryStep"; }
	};

	struct QueryPlan : std::vector<QueryStep *> {
		inline QueryPlan() = default;

		inline QueryPlan(const QueryPlan &) = delete;

		inline QueryPlan(QueryPlan &&other) : std::vector<QueryStep *>(std::move(other)){
		}

		inline ~QueryPlan() {
			for (auto iter = begin(); iter != end(); ++iter) {
				delete *iter;
			}
		}
	};

	/* TODO: query nesting
	struct QueryOptimizer {
		Schema &_schema;

		inline QueryOptimizer(Schema &schema) : _schema(_schema) {
		}

		inline QueryPlan generate(Query &query) {
			auto  c= [](Tuple &tuple) {
				return tuple.get<int_t>(3) < 5;
			};
			auto  d = [](Tuple &tuple) {
				return tuple.get<int_t>(3) > 1;
			};

			auto  x = [c, d](Tuple &tuple) {
				return c(tuple) && d(tuple);
			};
			UnaryQueryStep s;
			s._selection = c;
			return QueryPlan();
		}
	};*/
}
