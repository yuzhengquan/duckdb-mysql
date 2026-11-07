#include "mysql_connection.hpp"

#include "duckdb.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "mysql_types.hpp"

namespace duckdb {

static bool debug_mysql_print_queries = false;

MySQLConnection::MySQLConnection(shared_ptr<OwnedMySQLConnection> connection_p, MySQLTypeConfig type_config_p,
                                 const string &connection_string_p)
    : connection(std::move(connection_p)), type_config(std::move(type_config_p)),
      connection_string(connection_string_p) {
}

MySQLConnection::~MySQLConnection() {
	Close();
}

MySQLConnection::MySQLConnection(MySQLConnection &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(type_config, other.type_config);
	std::swap(connection_string, other.connection_string);
}

MySQLConnection &MySQLConnection::operator=(MySQLConnection &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(type_config, other.type_config);
	std::swap(connection_string, other.connection_string);
	return *this;
}

MySQLConnection MySQLConnection::Open(MySQLTypeConfig type_config, const string &connection_string,
                                      const string &attach_path) {
	auto connection = make_shared_ptr<OwnedMySQLConnection>(MySQLUtils::Connect(connection_string, attach_path));
	return MySQLConnection(std::move(connection), std::move(type_config), connection_string);
}



// Reads and discards all remaining rows of a streamed result, so the connection can be used for other queries
static void DrainResult(MySQLResult &result) {
	while (!result.Exhausted()) {
		result.NextChunk();
	}
}


bool MySQLConnection::IsOpen() {
	return connection.get() != nullptr;
}

void MySQLConnection::Close() {
	if (!IsOpen()) {
		return;
	}
	connection = nullptr;
}

vector<IndexInfo> MySQLConnection::GetIndexInfo(const string &table_name) {
	return vector<IndexInfo>();
}

void MySQLConnection::DebugSetPrintQueries(bool print) {
	debug_mysql_print_queries = print;
}

bool MySQLConnection::DebugPrintQueries() {
	return debug_mysql_print_queries;
}

bool MySQLConnection::TryCancelQuery(ClientContext &context, const MySQLTypeConfig &type_config,
                                     const string &connection_string, unsigned long connection_id) {
	if (connection_string.empty()) {
		return false;
	}
	try {
		auto con = MySQLConnection::Open(type_config, connection_string, "");
		string kill_query = "KILL QUERY " + to_string(connection_id);
		con.Execute(context, kill_query);
		return true;
	} catch (std::exception &) {
		return false;
	}
}


void MySQLConnection::Reset() {
	lock_guard<mutex> guard(query_lock);

	MYSQL *mysql_conn = GetConn();

#if defined(MARIADB_VERSION_ID) || (defined(MYSQL_VERSION_ID) && MYSQL_VERSION_ID >= 50703)
	if (mysql_reset_connection(mysql_conn) != 0) {
		throw IOException("Failed to reset MySQL connection: %s", mysql_error(mysql_conn));
	}
#else
	if (mysql_change_user(mysql_conn, nullptr, nullptr, nullptr) != 0) {
		throw IOException("Failed to reset MySQL connection: %s", mysql_error(mysql_conn));
	}
#endif

	if (mysql_query(mysql_conn, "SET autocommit=1") != 0) {
		throw IOException("Failed to set autocommit after connection reset: %s", mysql_error(mysql_conn));
	}

	if (mysql_set_character_set(mysql_conn, "utf8mb4") != 0) {
		throw IOException("Failed to set character set after connection reset: %s", mysql_error(mysql_conn));
	}

	const char *charset = mysql_character_set_name(mysql_conn);
	if (strcmp(charset, "utf8mb4") != 0) {
		throw IOException("Character set verification failed: expected utf8mb4, got %s", charset);
	}
}

string MySQLConnection::GetServerInfo() {
	lock_guard<mutex> guard(query_lock);
	auto conn = GetConn();
	const char *server_info = mysql_get_server_info(conn);
	return server_info != nullptr ? string(server_info) : string();
}

static string ValueToSQLLiteral(const Value &value) {
	if (value.IsNull()) {
		return "NULL";
	}
	switch (value.type().id()) {
	case LogicalTypeId::BOOLEAN:
		return value.GetValue<bool>() ? "1" : "0";
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		return to_string(value.GetValue<int64_t>());
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		return to_string(value.GetValue<uint64_t>());
	case LogicalTypeId::FLOAT:
		return to_string(value.GetValue<float>());
	case LogicalTypeId::DOUBLE:
		return to_string(value.GetValue<double>());
	case LogicalTypeId::VARCHAR:
		return MySQLUtils::WriteLiteral(StringValue::Get(value));
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return MySQLUtils::WriteLiteral(value.ToString());
	default:
		throw IOException("Unsupported parameters type: \"%s\"", value.type());
	}
}

static string InlineParams(const string &query, const vector<Value> &params) {
	string result = query;
	size_t pos = 0;
	for (const Value &param : params) {
		size_t found = result.find('?', pos);
		if (found == string::npos) {
			throw IOException("MySQL query has %zu parameters, but the query contains fewer '?' placeholders",
			                  params.size());
		}
		result.replace(found, 1, ValueToSQLLiteral(param));
		pos = found + 1;
	}
	return result;
}

unique_ptr<MySQLResult> MySQLConnection::QueryInternal(optional_ptr<ClientContext> context, const string &query,
                                                       const vector<Value> &params, MySQLResultStreaming streaming) {
	auto con = GetConn();
	lock_guard<mutex> guard(query_lock);

	string sql = query;
	if (!params.empty()) {
		sql = InlineParams(query, params);
	}

	if (MySQLConnection::DebugPrintQueries()) {
		string msg = StringUtil::Format("%s /* streaming=%s */\n",
		                                sql, streaming == MySQLResultStreaming::ALLOW_STREAMING ? "TRUE" : "FALSE");
		Printer::Print(msg);
	}

	int res_query = mysql_real_query(con, sql.c_str(), sql.size());
	if (res_query != 0) {
		throw IOException("Failed to run query \"%s\": %s\n", sql.c_str(), mysql_error(con));
	}

	unsigned long long affected_rows = mysql_affected_rows(con);
	if (affected_rows == (unsigned long long)-1) {
		// the query produced a result set, read it with the unbuffered interface
		MYSQL_RES *res = mysql_use_result(con);
		if (!res) {
			throw IOException("Failed to get result for MySQL query \"%s\": %s\n", sql.c_str(), mysql_error(con));
		}
		return make_uniq<MySQLResult>(context, query, MySQLResultPtr(res, MySQLResultDelete), type_config,
		                              connection_string, connection->GetID(), streaming, static_cast<idx_t>(-1), con);
	}
	// DML query without a result set
	return make_uniq<MySQLResult>(context, query, MySQLResultPtr(nullptr, MySQLResultDelete), type_config, connection_string, connection->GetID(),
	                              streaming, static_cast<idx_t>(affected_rows), con);
}

unique_ptr<MySQLResult> MySQLConnection::Query(ClientContext &context, const string &query,
                                               MySQLResultStreaming streaming) {
	return QueryInternal(context, query, vector<Value>(), streaming);
}

unique_ptr<MySQLResult> MySQLConnection::Query(ClientContext &context, const string &query, const vector<Value> &params,
                                               MySQLResultStreaming streaming) {
	return QueryInternal(context, query, params, streaming);
}

void MySQLConnection::Initialize(ClientContext &context, const MySQLConnectionInitOptions &options) {
	SetTypeConfig(options.type_config);

	if (!options.time_zone.empty()) {
		Execute(context, "SET TIME_ZONE = " + MySQLUtils::WriteLiteral(options.time_zone));
	}
}

void MySQLConnection::Execute(ClientContext &context, const string &query) {
	Execute(context, query, vector<Value>());
}

void MySQLConnection::Execute(ClientContext &context, const string &query, const vector<Value> &params) {
	// the result is fetched unbuffered and drained immediately
	auto result = QueryInternal(context, query, params, MySQLResultStreaming::ALLOW_STREAMING);
	DrainResult(*result);
}

bool MySQLConnection::IsConnectionHealthy(const string &health_check_query) {
	if (!IsOpen()) {
		return false;
	}

	if (!health_check_query.empty()) {
		try {
			// called by the connection pool, no context is available, the result is fetched unbuffered and
			// drained immediately, so the connection can be used for other queries
			auto result =
			    QueryInternal(nullptr, health_check_query, vector<Value>(), MySQLResultStreaming::ALLOW_STREAMING);
			DrainResult(*result);
			return true;
		} catch (...) {
			return false;
		}
	}

	lock_guard<mutex> guard(query_lock);

	MYSQL *mysql_conn = GetConn();
	if (mysql_ping(mysql_conn) != 0) {
		unsigned int err = mysql_errno(mysql_conn);
		(void)err;
		return false;
	}
	return true;
}
} // namespace duckdb
