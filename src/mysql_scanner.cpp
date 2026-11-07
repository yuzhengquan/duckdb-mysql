#include "mysql_scanner.hpp"

#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/planner/binder.hpp"

#include "dbconnector/defer.hpp"

#include "mysql_filter_pushdown.hpp"
#include "mysql_result.hpp"
#include "storage/mysql_catalog.hpp"
#include "storage/mysql_connection_pool.hpp"
#include "storage/mysql_table_set.hpp"
#include "storage/mysql_transaction.hpp"

namespace duckdb {

struct MySQLLocalState : public LocalTableFunctionState {};

enum class MySQLQueryExecState { UNINITIALIZED, EXECUTED, EXHAUSTED };

struct MySQLGlobalState : public GlobalTableFunctionState {
	explicit MySQLGlobalState(MySQLPooledConnection pinned_connection_p)
	    : pinned_connection(std::move(pinned_connection_p)) {
	}

	explicit MySQLGlobalState(string scan_query_p) : scan_query(std::move(scan_query_p)) {
	}

	~MySQLGlobalState() {
		if (!pinned_connection) {
			return;
		}
		try {
			pinned_connection.PinBack();
		} catch (...) {
			// suppress
		}
	}

	MySQLPooledConnection pinned_connection;
	vector<Value> params;
	string scan_query;
	unique_ptr<MySQLResult> result;
	MySQLQueryExecState exec_state = MySQLQueryExecState::UNINITIALIZED;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> MySQLBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<Identifier> &names) {
	throw InternalException("MySQLBind");
}

static unique_ptr<GlobalTableFunctionState> MySQLInitGlobalState(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<MySQLBindData>();

	string select;
	select += "SELECT ";
	for (idx_t c = 0; c < input.column_ids.size(); c++) {
		if (c > 0) {
			select += ", ";
		}
		if (input.column_ids[c] == COLUMN_IDENTIFIER_ROW_ID) {
			select += "NULL";
		} else {
			auto &col = bind_data.columns.GetColumn(LogicalIndex(input.column_ids[c]));
			auto col_name = col.GetName();
			select += MySQLUtils::WriteIdentifier(col_name.GetIdentifierName());
		}
	}
	select += " FROM ";
	select += MySQLUtils::WriteIdentifier(bind_data.table_name.Schema().GetIdentifierName());
	select += ".";
	select += MySQLUtils::WriteIdentifier(bind_data.table_name.Name().GetIdentifierName());

	auto attached_catalog = MySQLCatalog::Lookup(context, bind_data.table_name.Catalog());
	MySQLFilterPushdown pushdown(std::move(attached_catalog));
	string filter_string = pushdown.TransformFilters(input.column_ids, input.filters, bind_data.names);

	if (!filter_string.empty()) {
		select += " WHERE " + filter_string;
	}

	return make_uniq<MySQLGlobalState>(std::move(select));
}

static unique_ptr<LocalTableFunctionState> MySQLInitLocalState(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
	return make_uniq<MySQLLocalState>();
}

static void MySQLScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<MySQLGlobalState>();

	if (gstate.exec_state == MySQLQueryExecState::UNINITIALIZED) {
		auto &bdata = data.bind_data->Cast<MySQLBindData>();
		auto attached_table = MySQLTableEntry::Lookup(context, bdata.table_name);
		MySQLTableEntry &table_entry = attached_table.Get<MySQLTableEntry>();
		auto &transaction = MySQLTransaction::Get(context, table_entry.ParentCatalog());
		auto &con = transaction.GetConnection();
		gstate.result = con.Query(context, gstate.scan_query, bdata.optimizer_streaming);
		gstate.exec_state = MySQLQueryExecState::EXECUTED;
	}

	while (true) {
		if (gstate.result->Exhausted()) {
			output.SetChildCardinality(0);
			return;
		}

		DataChunk &res_chunk = gstate.result->NextChunk();
		D_ASSERT(output.ColumnCount() == res_chunk.ColumnCount());
		string error;
		for (idx_t c = 0; c < output.ColumnCount(); c++) {
			Vector &output_vec = output.data[c];
			Vector &res_vec = res_chunk.data[c];
			switch (output_vec.GetType().id()) {
			case LogicalTypeId::BOOLEAN:
			case LogicalTypeId::TINYINT:
			case LogicalTypeId::UTINYINT:
			case LogicalTypeId::SMALLINT:
			case LogicalTypeId::USMALLINT:
			case LogicalTypeId::INTEGER:
			case LogicalTypeId::UINTEGER:
			case LogicalTypeId::BIGINT:
			case LogicalTypeId::UBIGINT:
			case LogicalTypeId::FLOAT:
			case LogicalTypeId::DOUBLE:
			case LogicalTypeId::BLOB:
			case LogicalTypeId::DATE:
			case LogicalTypeId::TIME:
			case LogicalTypeId::TIMESTAMP: {
				if (output_vec.GetType().id() == res_vec.GetType().id() ||
				    (output_vec.GetType().id() == LogicalTypeId::BLOB &&
				     res_vec.GetType().id() == LogicalTypeId::VARCHAR)) {
					output_vec.Reinterpret(res_vec);
				} else {
					VectorOperations::TryCast(context, res_vec, output_vec, res_chunk.size(), &error);
				}
				break;
			}
			default: {
				VectorOperations::TryCast(context, res_vec, output_vec, res_chunk.size(), &error);
				break;
			}
			}
			if (!error.empty()) {
				throw BinderException(error);
			}
		}
		output.SetChildCardinality(res_chunk.size());
		return;
	}
}

static InsertionOrderPreservingMap<string> MySQLScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<MySQLBindData>();
	result["Table"] = bind_data.table_name.Name().GetIdentifierName();
	return result;
}

static void MySQLScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                               const BoundTableFunction &function) {
	throw NotImplementedException("MySQLScanSerialize");
}

static unique_ptr<FunctionData> MySQLScanDeserialize(Deserializer &deserializer, BoundTableFunction &function) {
	throw NotImplementedException("MySQLScanDeserialize");
}

static optional_ptr<TableCatalogEntry> MySQLGetTableEntry(optional_ptr<const FunctionData> bind_data_p) {
	auto &bdata = bind_data_p->Cast<MySQLBindData>();
	shared_ptr<ClientContext> ctx = bdata.context_ptr.lock();
	if (!ctx) { // cannot fail in known scenarios
		return nullptr;
	}
	auto attached_table = MySQLTableEntry::Lookup(*ctx, bdata.table_name);
	return attached_table.Get<MySQLTableEntry>();
}

MySQLScanFunction::MySQLScanFunction()
    : TableFunction("mysql_scan",
                    FunctionSignature()
                        .AddPositionalOnly("database", LogicalType::VARCHAR)
                        .AddPositionalOnly("schema_name", LogicalType::VARCHAR)
                        .AddPositionalOnly("table_name", LogicalType::VARCHAR),
                    MySQLScan, MySQLBind, MySQLInitGlobalState, MySQLInitLocalState) {
	to_string = MySQLScanToString;
	serialize = MySQLScanSerialize;
	deserialize = MySQLScanDeserialize;
	get_table_entry = MySQLGetTableEntry;
	projection_pushdown = true;
}

//===--------------------------------------------------------------------===//
// MySQL Query
//===--------------------------------------------------------------------===//

static MySQLCatalog &GetCatalogByName(ClientContext &context, const std::string &name) {
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, Identifier(name));
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\"", name);
	}
	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "mysql") {
		throw BinderException("Attached database \"%s\" does not refer to a MySQL database", name);
	}
	return catalog.Cast<MySQLCatalog>();
}

static void MySQLPinConnection(DataChunk &args, ExpressionState &state, Vector &result) {
	UnifiedVectorFormat catalog_name_data;
	args.data[0].ToUnifiedFormat(catalog_name_data);
	if (!catalog_name_data.validity.RowIsValid(0)) {
		throw InvalidInputException("Specified attached database name must be not null");
	}

	auto catalog_name_strt = UnifiedVectorFormat::GetData<string_t>(catalog_name_data)[0];
	string catalog_name(catalog_name_strt.GetData(), catalog_name_strt.GetSize());

	auto &catalog = GetCatalogByName(state.GetContext(), catalog_name);
	auto &pool = catalog.GetConnectionPool();

	auto conn = pool.ForceAcquire();
	uint64_t conn_id = pool.PinConnection(std::move(conn));

	auto result_data = FlatVector::GetDataMutable<uint64_t>(result);
	result_data[0] = conn_id;
}

MySQLPinConnectionFunction::MySQLPinConnectionFunction()
    : ScalarFunction("mysql_pin_connection", {LogicalType::VARCHAR}, LogicalType::UBIGINT, MySQLPinConnection) {
	SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	SetFallible();
	SetVolatile();
}

static void MySQLClosePinnedConnection(DataChunk &args, ExpressionState &state, Vector &result) {
	UnifiedVectorFormat catalog_name_data;
	args.data[0].ToUnifiedFormat(catalog_name_data);
	if (!catalog_name_data.validity.RowIsValid(0)) {
		throw InvalidInputException("Specified catalog name must be not null");
	}
	auto catalog_name_strt = UnifiedVectorFormat::GetData<string_t>(catalog_name_data)[0];
	string catalog_name(catalog_name_strt.GetData(), catalog_name_strt.GetSize());

	UnifiedVectorFormat conn_id_data;
	args.data[1].ToUnifiedFormat(conn_id_data);
	if (!conn_id_data.validity.RowIsValid(0)) {
		throw InvalidInputException("Specified connection ID must be not null");
	}
	auto conn_id = UnifiedVectorFormat::GetData<uint64_t>(conn_id_data)[0];

	auto &catalog = GetCatalogByName(state.GetContext(), catalog_name);
	{ auto conn = catalog.GetConnectionPool().UnpinConnection(conn_id); }

	auto &result_validity = FlatVector::ValidityMutable(result);
	result_validity.SetInvalid(0);
}

MySQLClosePinnedConnectionFunction::MySQLClosePinnedConnectionFunction()
    : ScalarFunction("mysql_close_pinned_connection", {LogicalType::VARCHAR, LogicalType::UBIGINT},
                     LogicalType::BOOLEAN, MySQLClosePinnedConnection) {
	SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	SetFallible();
	SetVolatile();
}


} // namespace duckdb
