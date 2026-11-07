#include "mysql_field.hpp"

#include "mysql_result.hpp"

namespace duckdb {

MySQLField::MySQLField(MYSQL_FIELD *mf, LogicalType duckdb_type_p, const MySQLTypeConfig &type_config)
    : name(mf->name), mysql_type(mf->type), duckdb_type(std::move(duckdb_type_p)) {
}

vector<MySQLField> MySQLField::ReadFields(const std::string &query, MYSQL_RES *result,
                                          const MySQLTypeConfig &type_config) {
	vector<MySQLField> fields;
	idx_t field_count = mysql_num_fields(result);
	if (field_count == 0) {
		return fields;
	}
	MYSQL_FIELD *mfields = mysql_fetch_fields(result);
	if (!mfields) {
		throw IOException("Failed to fetch result fields for MySQL query \"%s\"", query);
	}
	fields.reserve(field_count);
	for (size_t i = 0; i < field_count; i++) {
		auto &mf = mfields[i];
		LogicalType ltype = MySQLTypes::FieldToLogicalType(type_config, &mf);
		MySQLField msf(&mf, std::move(ltype), type_config);
		fields.emplace_back(std::move(msf));
	}
	return fields;
}

} // namespace duckdb
