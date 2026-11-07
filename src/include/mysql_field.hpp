//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_field.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "mysql_types.hpp"

namespace duckdb {

struct MySQLField {
	string name;
	enum_field_types mysql_type;
	LogicalType duckdb_type;


	MySQLField(MYSQL_FIELD *mf, LogicalType duckdb_type_p, const MySQLTypeConfig &type_config);


	static vector<MySQLField> ReadFields(const std::string &query, MYSQL_RES *result,
	                                     const MySQLTypeConfig &type_config);
};

} // namespace duckdb
