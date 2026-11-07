//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"

#include "mysql_connection.hpp"
#include "mysql_types.hpp"
#include "mysql_utils.hpp"
#include "storage/mysql_catalog.hpp"

namespace duckdb {

struct MySQLBindData : public FunctionData {
	explicit MySQLBindData(MySQLTableEntry &table, weak_ptr<ClientContext> ctx)
	    : table_name(table.ParentCatalog().GetName(), table.ParentSchema().name, table.name),
	      columns(table.GetColumns().Copy()), context_ptr(std::move(ctx)) {
	}

	QualifiedName table_name;
	ColumnList columns;

	// required for get_bind_info and only used there
	weak_ptr<ClientContext> context_ptr;

	vector<MySQLType> mysql_types;
	vector<string> names;
	vector<LogicalType> types;
	MySQLResultStreaming optimizer_streaming = MySQLResultStreaming::UNINITIALIZED;

public:
	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("MySQLBindData copy not supported");
	}
	bool Equals(const FunctionData &other_p) const override {
		return false;
	}
};


class MySQLScanFunction : public TableFunction {
public:
	MySQLScanFunction();
};


class MySQLClearCacheFunction : public TableFunction {
public:
	MySQLClearCacheFunction();

	static void ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter);
};


class MySQLPinConnectionFunction : public ScalarFunction {
public:
	MySQLPinConnectionFunction();
};

class MySQLClosePinnedConnectionFunction : public ScalarFunction {
public:
	MySQLClosePinnedConnectionFunction();
};



} // namespace duckdb
