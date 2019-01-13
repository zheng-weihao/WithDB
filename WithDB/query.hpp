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
		size_t _result;
	};

	struct UnaryQueryStep {
		size_t _single;
		std::function<bool(Tuple &)> _selection;
		std::vector<size_t> _projection;
	};

	struct BinaryQueryStep {
		size_t _left, _right;
		std::function<bool(Tuple &)> _leftSelection;
		std::function<bool(Tuple &)> _rightSelection;
		std::function<bool(Tuple &, Tuple &)> _join;
		std::vector<size_t> _leftProjection;
		std::vector<size_t> _rightProjection;
	};

	struct QueryPlan : std::vector<QueryStep *> {
		inline ~QueryPlan() {
			for (auto iter = begin(); iter != end(); ++iter) {
				delete *iter;
			}
		}
	};

	// TODO: query nesting
	struct QueryOptimizer {
		Schema &_schema;

		inline QueryOptimizer(Schema &schema) : _schema(_schema) {
		}

		inline QueryPlan generate(Query &query) {
			auto root = query._root;
		}
	};
}
