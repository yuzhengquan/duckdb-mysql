#include "mysql_result.hpp"

#include <cstdint>

#include <limits>

#include "duckdb.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include "mysql_connection.hpp"

namespace duckdb {

static vector<LogicalType> CreateChunkTypes(vector<MySQLField> &fields, const MySQLTypeConfig &type_config) {
	vector<LogicalType> ltypes;
	for (idx_t c = 0; c < fields.size(); c++) {
		MySQLField &f = fields[c];
		LogicalType &lt = f.duckdb_type;
		switch (lt.id()) {
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
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIMESTAMP:
			ltypes.push_back(lt);
			break;
		case LogicalTypeId::DOUBLE: {
			if (f.mysql_type == MYSQL_TYPE_DOUBLE) {
				ltypes.push_back(lt);
			} else {
				ltypes.push_back(LogicalType::VARCHAR);
			}
			break;
		}
		case LogicalTypeId::TIME: {
			if (type_config.time_as_time) {
				ltypes.push_back(lt);
			} else {
				ltypes.push_back(LogicalType::VARCHAR);
			}
			break;
		}
		default:
			ltypes.push_back(LogicalType::VARCHAR);
		}
	}
	return ltypes;
}




void MySQLResult::Materialize() {
	if (!context) {
		throw InternalException("MySQLResult::Materialize called without a context, MySQL query \"%s\"", query);
	}
	if (exhausted) {
		return;
	}
	auto collection = make_uniq<ColumnDataCollection>(BufferManager::GetBufferManager(*context), data_chunk.GetTypes());
	ColumnDataAppendState append_state;
	collection->InitializeAppend(append_state);
	try {
		while (!exhausted) {
			FetchChunk();
			collection->Append(append_state, data_chunk);
		}
	} catch (...) {
		// the remaining rows must be discarded from the connection before it can be used for other queries
		TryCancelQuery();
		throw;
	}
	// reset to the initial state, so the first Next() call scans the first chunk
	data_chunk.Reset();
	row_idx = static_cast<idx_t>(-1);
	materialized = std::move(collection);
	materialized->InitializeScan(materialized_scan_state);
	exhausted = materialized->Count() == 0;
}

DataChunk &MySQLResult::NextChunk() {
	if (!materialized) {
		FetchChunk();
		return this->data_chunk;
	}
	this->row_idx = 0;
	context->InterruptCheck();
	materialized->Scan(materialized_scan_state, this->data_chunk);
	materialized_rows_scanned += this->data_chunk.size();
	if (materialized_rows_scanned >= materialized->Count()) {
		this->exhausted = true;
	}
	return this->data_chunk;
}

void MySQLResult::FetchChunk() {
	// the context is not available only for the health check queries
	if (context) {
		context->InterruptCheck();
	}
	this->data_chunk.Reset();
	this->row_idx = 0;

	idx_t r = 0;
	for (; r < STANDARD_VECTOR_SIZE; r++) {
		if (!FetchNext()) {
			this->exhausted = true;
			break;
		}

		WriteToChunk(r);
	}

	this->data_chunk.SetChildCardinality(r);
}

bool MySQLResult::Exhausted() {
	return this->exhausted;
}


bool MySQLResult::Next() {
	if (row_idx < data_chunk.size() - 1) {
		row_idx += 1;
		return true;
	}
	NextChunk();
	row_idx = 0;
	return data_chunk.size() > 0;
}

void MySQLResult::CheckColumnIdx(idx_t col) {
	if (col >= data_chunk.ColumnCount()) {
		throw IOException("Column: %zu out of range of field count: %zu, MySQL query \"%s\"\n", col,
		                  data_chunk.ColumnCount(), query.c_str());
	}
}

void MySQLResult::CheckNotNull(idx_t col) {
	if (IsNull(col)) {
		throw InternalException("Get called for a NULL value, column: %zu, MySQL query \"%s\"\n", col, query.c_str());
	}
}

string MySQLResult::GetString(idx_t col) {
	CheckNotNull(col);
	MySQLField &f = fields[col];
	if (f.duckdb_type.id() == LogicalTypeId::VARCHAR || f.duckdb_type.id() == LogicalTypeId::BLOB) {
		Vector &vec = data_chunk.data[col];
		string_t *data = FlatVector::GetDataMutable<string_t>(vec);
		string_t &st = data[row_idx];
		return string(st.GetData(), st.GetSize());
	}
	throw InternalException("Get called for a String type, actual type: \"%s\", column: %zu, MySQL query \"%s\"\n",
	                        f.duckdb_type.ToString(), col, query.c_str());
}

int32_t MySQLResult::GetInt32(idx_t col) {
	CheckNotNull(col);
	MySQLField &f = fields[col];
	Vector &vec = data_chunk.data[col];
	if (f.duckdb_type.id() == LogicalTypeId::INTEGER) {
		int32_t *data = FlatVector::GetDataMutable<int32_t>(vec);
		return data[row_idx];
	} else if (f.duckdb_type.id() == LogicalTypeId::UINTEGER) {
		uint32_t *data = FlatVector::GetDataMutable<uint32_t>(vec);
		return static_cast<int32_t>(data[row_idx]);
	}
	throw InternalException("Get called for an Int32 type, actual type: \"%s\", column: %zu, MySQL query \"%s\"\n",
	                        f.duckdb_type.ToString(), col, query.c_str());
}

int64_t MySQLResult::GetInt64(idx_t col) {
	CheckNotNull(col);
	MySQLField &f = fields[col];
	Vector &vec = data_chunk.data[col];
	if (f.duckdb_type.id() == LogicalTypeId::BIGINT) {
		int64_t *data = FlatVector::GetDataMutable<int64_t>(vec);
		return data[row_idx];
	} else if (f.duckdb_type.id() == LogicalTypeId::UBIGINT) {
		uint64_t *data = FlatVector::GetDataMutable<uint64_t>(vec);
		return static_cast<int64_t>(data[row_idx]);
	} else if (f.duckdb_type.id() == LogicalTypeId::DOUBLE) {
		if (vec.GetType().id() == LogicalTypeId::DOUBLE) {
			double *data = FlatVector::GetDataMutable<double>(vec);
			return static_cast<uint64_t>(data[row_idx]);
		} else if (vec.GetType().id() == LogicalTypeId::VARCHAR) {
			string_t *data = FlatVector::GetDataMutable<string_t>(vec);
			string_t st = data[row_idx];
			return atoll(st.GetData());
		}
	}
	throw InternalException("Get called for an Int64 type, actual type: \"%s\", column: %zu, MySQL query \"%s\"\n",
	                        f.duckdb_type.ToString(), col, query.c_str());
}

bool MySQLResult::IsNull(idx_t col) {
	CheckColumnIdx(col);
	Vector &vec = data_chunk.data[col];
	return FlatVector::IsNull(vec, row_idx);
}

idx_t MySQLResult::AffectedRows() {
	if (affected_rows == idx_t(-1)) {
		throw InternalException("MySQLResult::AffectedRows called for result "
		                        "that didn't affect any rows, query \"%s\"\n",
		                        query.c_str());
	}
	return affected_rows;
}

int64_t MySQLResult::AffectedRowsSigned() {
	if (affected_rows > (std::numeric_limits<int64_t>::max)()) {
		return -1;
	}
	return static_cast<int64_t>(affected_rows);
}

const vector<MySQLField> &MySQLResult::Fields() {
	return fields;
}

bool MySQLResult::TryCancelQuery() {
	if (!context) {
		return false;
	}
	return MySQLConnection::TryCancelQuery(*context, type_config, connection_string, connection_id);
}



MySQLResult::MySQLResult(optional_ptr<ClientContext> context_p, const std::string &query_p, MySQLResultPtr result_p,
                         MySQLTypeConfig type_config_p, const string &connection_string_p,
                         unsigned long connection_id_p, MySQLResultStreaming streaming_p, idx_t affected_rows_p,
                         MYSQL *mysql_handle_p)
    : context(context_p), query(query_p), result(std::move(result_p)), type_config(std::move(type_config_p)),
      connection_string(connection_string_p), connection_id(connection_id_p), streaming(streaming_p),
      affected_rows(affected_rows_p), mysql_handle(mysql_handle_p) {
	if (affected_rows != static_cast<idx_t>(-1)) {
		this->exhausted = true;
		return;
	}
	if (!this->result) {
		throw InternalException("MySQL query result expected a result set, query \"%s\"", query);
	}
	this->fields = MySQLField::ReadFields(query, this->result.get(), type_config);
	auto ltypes = CreateChunkTypes(fields, type_config);
	this->data_chunk.Initialize(Allocator::DefaultAllocator(), ltypes);
}

MySQLResult::~MySQLResult() {
	bool stream_active = streaming == MySQLResultStreaming::ALLOW_STREAMING;
	if (stream_active && !exhausted) {
		TryCancelQuery();
	}
}

bool MySQLResult::FetchNext() {
	MYSQL_ROW row = mysql_fetch_row(result.get());
	if (!row) {
		if (mysql_handle && mysql_errno(mysql_handle) != 0) {
			throw IOException("Failed to fetch result row for MySQL query \"%s\": %s\n", query.c_str(),
			                  mysql_error(mysql_handle));
		}
		return false;
	}
	current_row = row;
	current_lengths = mysql_fetch_lengths(result.get());
	return true;
}

static void WriteString(MySQLField &f, Vector &vec, idx_t row, const char *str, idx_t len) {
	auto data = FlatVector::GetDataMutable<string_t>(vec);
	string_t st(str, len);
	data[row] = StringVector::AddStringOrBlob(vec, std::move(st));
}

template <typename NUM_TYPE>
static void WriteNumber(Vector &vec, idx_t row, const char *str) {
	auto data = FlatVector::GetDataMutable<NUM_TYPE>(vec);
	data[row] = static_cast<NUM_TYPE>(strtoll(str, nullptr, 10));
}

template <typename NUM_TYPE>
static void WriteUnsignedNumber(Vector &vec, idx_t row, const char *str) {
	auto data = FlatVector::GetDataMutable<NUM_TYPE>(vec);
	data[row] = static_cast<NUM_TYPE>(strtoull(str, nullptr, 10));
}

static void WriteDateTimeFromString(const string &str, Vector &vec, idx_t row) {
	try {
		switch (vec.GetType().id()) {
		case LogicalTypeId::DATE: {
			date_t val = Date::FromCString(str.c_str(), str.size());
			FlatVector::GetDataMutable<date_t>(vec)[row] = val;
			return;
		}
		case LogicalTypeId::TIME: {
			dtime_t val = Time::FromCString(str.c_str(), str.size());
			FlatVector::GetDataMutable<dtime_t>(vec)[row] = val;
			return;
		}
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_TZ: {
			timestamp_t val = Timestamp::FromCString(str.c_str(), str.size());
			FlatVector::GetDataMutable<timestamp_t>(vec)[row] = val;
			return;
		}
		default:
			break;
		}
	} catch (std::exception &) {
		// zero dates and incomplete dates are converted to NULL
		if (str.find("0000") != string::npos || str.find("-00-") != string::npos || str.find("-00 ") != string::npos) {
			FlatVector::SetNull(vec, row, true);
			return;
		}
		throw;
	}
	throw InternalException("Unsupported date/time type: " + vec.GetType().ToString());
}

void MySQLResult::WriteToChunk(idx_t row) {
	for (idx_t col = 0; col < data_chunk.ColumnCount(); col++) {
		MySQLField &f = fields[col];
		Vector &vec = data_chunk.data[col];
		if (current_row[col] == nullptr) {
			FlatVector::SetNull(vec, row, true);
			continue;
		}
		const char *str = current_row[col];
		idx_t len = current_lengths ? current_lengths[col] : strlen(str);
		switch (vec.GetType().id()) {
		case LogicalTypeId::BOOLEAN: {
			auto data = FlatVector::GetDataMutable<bool>(vec);
			data[row] = str[0] != '\0' && str[0] != '0' && str[0] != '-';
			break;
		}
		case LogicalTypeId::TINYINT:
			WriteNumber<int8_t>(vec, row, str);
			break;
		case LogicalTypeId::UTINYINT:
			WriteUnsignedNumber<uint8_t>(vec, row, str);
			break;
		case LogicalTypeId::SMALLINT:
			WriteNumber<int16_t>(vec, row, str);
			break;
		case LogicalTypeId::USMALLINT:
			WriteUnsignedNumber<uint16_t>(vec, row, str);
			break;
		case LogicalTypeId::INTEGER:
			WriteNumber<int32_t>(vec, row, str);
			break;
		case LogicalTypeId::UINTEGER:
			WriteUnsignedNumber<uint32_t>(vec, row, str);
			break;
		case LogicalTypeId::BIGINT:
			WriteNumber<int64_t>(vec, row, str);
			break;
		case LogicalTypeId::UBIGINT:
			WriteUnsignedNumber<uint64_t>(vec, row, str);
			break;
		case LogicalTypeId::FLOAT: {
			auto data = FlatVector::GetDataMutable<float>(vec);
			data[row] = strtof(str, nullptr);
			break;
		}
		case LogicalTypeId::DOUBLE: {
			auto data = FlatVector::GetDataMutable<double>(vec);
			data[row] = strtod(str, nullptr);
			break;
		}
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIME:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_TZ: {
			WriteDateTimeFromString(string(str, len), vec, row);
			break;
		}
		default: {
			WriteString(f, vec, row, str, len);
			break;
		}
		}
	}
}

} // namespace duckdb
