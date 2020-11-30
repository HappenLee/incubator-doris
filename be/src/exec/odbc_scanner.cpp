// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <boost/algorithm/string.hpp>
#include <codecvt>
#include <sqlext.h>

#include "exprs/expr.h"
#include "exec/odbc_scanner.h"
#include "common/logging.h"
#include "runtime/primitive_type.h"
#include "util/types.h"

static constexpr uint32_t SMALL_COLUMN_SIZE_BUFFER = 100;
// Now we only treat HLL, CHAR, VARCHAR as big column
static constexpr uint32_t BIG_COLUMN_SIZE_BUFFER = 65535;

static std::u16string utf8_to_wstring (const std::string& str)
{
    std::wstring_convert<std::codecvt_utf8<char16_t>, char16_t> utf8_ucs2_cvt;
    return utf8_ucs2_cvt.from_bytes(str);
}

namespace doris {

ODBCScanner::ODBCScanner(const ODBCScannerParam& param)
        : _connect_string(build_connect_string(param)),
          _type(param.type),
          _tuple_desc(param.tuple_desc),
          _output_expr_ctxs(std::move(param.output_expr_ctxs)),
          _is_open(false),
          _field_num(0),
          _row_count(0),
          _env(nullptr),
          _dbc(nullptr),
          _stmt(nullptr) {
}

ODBCScanner::~ODBCScanner() {
    if (_stmt != nullptr) {
        SQLFreeHandle(SQL_HANDLE_STMT, _stmt);
    }

    if (_dbc != nullptr) {
        SQLDisconnect(_dbc);
        SQLFreeHandle(SQL_HANDLE_DBC, _dbc);
    }

    if (_env != nullptr) {
        SQLFreeHandle(SQL_HANDLE_ENV, _env);
    }
}

Status ODBCScanner::open() {
    if (_is_open) {
        LOG(INFO) << "this scanner already opened";
        return Status::OK();
    }

    // Allocate an environment
    if (SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &_env) != SQL_SUCCESS) {
        return Status::InternalError("alloc env failed");
    }
    // We want ODBC 3 support
    ODBC_DISPOSE(_env, SQL_HANDLE_ENV, SQLSetEnvAttr(_env, SQL_ATTR_ODBC_VERSION, (void *) SQL_OV_ODBC3, 0), "set env attr");
    // Allocate a connection handle
    ODBC_DISPOSE(_env, SQL_HANDLE_ENV, SQLAllocHandle(SQL_HANDLE_DBC, _env, &_dbc), "alloc dbc");
    // Connect to the Database
    ODBC_DISPOSE(_dbc, SQL_HANDLE_DBC, SQLDriverConnect(_dbc, NULL, (SQLCHAR*)_connect_string.c_str(), SQL_NTS,
                           NULL, 0, NULL, SQL_DRIVER_COMPLETE_REQUIRED), "driver connect");

    LOG(INFO) << "connect success:" << _connect_string.substr(0, _connect_string.find("Pwd="));

    _is_open = true;
    return Status::OK();
}

Status ODBCScanner::query(const std::string& query) {
    if (!_is_open) {
        return Status::InternalError( "Query before open.");
    }

    // Allocate a statement handle
    ODBC_DISPOSE(_dbc, SQL_HANDLE_DBC, SQLAllocHandle(SQL_HANDLE_STMT, _dbc, &_stmt), "alloc statement");

    // Translate utf8 string to utf16 to use unicode codeing
    auto wquery = utf8_to_wstring(query);
    ODBC_DISPOSE(_stmt, SQL_HANDLE_STMT, SQLExecDirectW(_stmt, (SQLWCHAR*)(wquery.c_str()), SQL_NTS), "exec direct");

    // How many columns are there */
    ODBC_DISPOSE(_stmt, SQL_HANDLE_STMT, SQLNumResultCols(_stmt, &_field_num), "count num colomn");

    LOG(INFO) << "execute success:" << query <<  " column count:" << _field_num;

    // check materialize num equal _field_num
    int materialize_num = 0;
    for (int i = 0; i < _tuple_desc->slots().size(); ++i) {
        if (_tuple_desc->slots()[i]->is_materialized()) {
            materialize_num++;
        }
    }
    if (_field_num != materialize_num) {
        return Status::InternalError("input and output not equal.");
    }

    // allocate memory for the binding
    for (int i = 0 ; i < _field_num ; i++ ) {
        DataBinding* column_data = new DataBinding;
        column_data->target_type = SQL_C_CHAR;
        auto type = _tuple_desc->slots()[i]->type().type;
        column_data->buffer_length = (type == TYPE_HLL || type == TYPE_CHAR || type == TYPE_VARCHAR) ? BIG_COLUMN_SIZE_BUFFER :
                SMALL_COLUMN_SIZE_BUFFER;
        column_data->target_value_ptr = malloc(sizeof(char) * column_data->buffer_length);
        _columns_data.push_back(column_data);
    }

    // setup the binding
    for (int i = 0 ; i < _field_num ; i++ ) {
        ODBC_DISPOSE(_stmt, SQL_HANDLE_STMT, SQLBindCol(_stmt, (SQLUSMALLINT)i + 1, _columns_data[i].target_type,
                              _columns_data[i].target_value_ptr, _columns_data[i].buffer_length, &(_columns_data[i].strlen_or_ind)), "bind col");
    }

    return Status::OK();
}

Status ODBCScanner::query(const std::string& table, const std::vector<std::string>& fields,
                       const std::vector<std::string>& filters) {
    if (!_is_open) {
        return Status::InternalError("Query before open.");
    }

    _sql_str = "SELECT ";

    for (int i = 0; i < fields.size(); ++i) {
        if (0 != i) {
            _sql_str += ",";
        }

        _sql_str += fields[i];
    }

    _sql_str += " FROM " + table;

    if (!filters.empty()) {
        _sql_str += " WHERE ";

        for (int i = 0; i < filters.size(); ++i) {
            if (0 != i) {
                _sql_str += " AND";
            }

            _sql_str += " (" + filters[i] + ") ";
        }
    }

    return query(_sql_str);
}

Status ODBCScanner::get_next_row(bool* eos) {
    if (!_is_open) {
        return Status::InternalError("GetNextRow before open.");
    }

    auto ret = SQLFetch(_stmt);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        return Status::OK();
    } else if (ret != SQL_NO_DATA_FOUND) {
        return error_status("result fetch", handle_diagnostic_record(_stmt, SQL_HANDLE_STMT, ret));
    }

    *eos = true;
    return Status::OK();
}

Status ODBCScanner::init_to_write() {
    if (!_is_open) {
        return Status::InternalError( "Init before open.");
    }

    // Allocate a statement handle
    ODBC_DISPOSE(_dbc, SQL_HANDLE_DBC, SQLAllocHandle(SQL_HANDLE_STMT, _dbc, &_stmt), "alloc statement");

    return Status::OK();
}

Status ODBCScanner::append(const std::string& table_name, doris::RowBatch *batch) {
    if (batch == nullptr || batch->num_rows() == 0) {
        return Status::OK();
    }

    int num_rows = batch->num_rows();
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(insert_row(table_name, batch->get_row(i)));
    }

    return Status::OK();
}

Status ODBCScanner::insert_row(const std::string& table_name, doris::TupleRow *row) {
    std::stringstream ss;

    // Construct Insert statement of mysql
    ss << "INSERT INTO " << table_name << " VALUES (";
    int num_columns = _output_expr_ctxs.size();
    for (int i = 0; i < num_columns; ++i) {
        if (i != 0) {
            ss << ", ";
        }
        void* item = _output_expr_ctxs[i]->get_value(row);
        if (item == nullptr) {
            ss << "NULL";
            continue;
        }
        switch (_output_expr_ctxs[i]->root()->type().type) {
            case TYPE_BOOLEAN:
            case TYPE_TINYINT:
                ss << (int)*static_cast<int8_t*>(item);
                break;
            case TYPE_SMALLINT:
                ss << *static_cast<int16_t*>(item);
                break;
            case TYPE_INT:
                ss << *static_cast<int32_t*>(item);
                break;
            case TYPE_BIGINT:
                ss << *static_cast<int64_t*>(item);
                break;
            case TYPE_FLOAT:
                ss << *static_cast<float*>(item);
                break;
            case TYPE_DOUBLE:
                ss << *static_cast<double*>(item);
                break;
            case TYPE_DATE:
            case TYPE_DATETIME: {
                char buf[64];
                const DateTimeValue* time_val = (const DateTimeValue*)(item);
                time_val->to_string(buf);
                ss << "\'" << buf << "\'";
                break;
            }
            case TYPE_VARCHAR:
            case TYPE_CHAR: {
                const StringValue* string_val = (const StringValue*)(item);

                if (string_val->ptr == NULL) {
                    if (string_val->len == 0) {
                        ss << "\'\'";
                    } else {
                        ss << "NULL";
                    }
                } else {
                    ss << "\'";
                    for (int j = 0; j < string_val->len ; ++j) {
                        ss << string_val->ptr[j];
                    }
                    ss << "\'";
                }
                break;
            }
            case TYPE_DECIMAL: {
                const DecimalValue* decimal_val = reinterpret_cast<const DecimalValue*>(item);
                std::string decimal_str;
                int output_scale = _output_expr_ctxs[i]->root()->output_scale();

                if (output_scale > 0 && output_scale <= 30) {
                    decimal_str = decimal_val->to_string(output_scale);
                } else {
                    decimal_str = decimal_val->to_string();
                }
                ss << decimal_str;
                break;
            }
            case TYPE_DECIMALV2: {
                const DecimalV2Value decimal_val(reinterpret_cast<const PackedInt128*>(item)->value);
                std::string decimal_str;
                int output_scale = _output_expr_ctxs[i]->root()->output_scale();

                if (output_scale > 0 && output_scale <= 30) {
                    decimal_str = decimal_val.to_string(output_scale);
                } else {
                    decimal_str = decimal_val.to_string();
                }
                ss << decimal_str;
                break;
            }

            default: {
                std::stringstream err_ss;
                err_ss << "can't convert this type to mysql type. type = " <<
                       _output_expr_ctxs[i]->root()->type();
                return Status::InternalError(err_ss.str());
            }
        }
    }
    ss << ")";

    // Translate utf8 string to utf16 to use unicode codeing
    auto insert_stmt = utf8_to_wstring(ss.str());
    ODBC_DISPOSE(_stmt, SQL_HANDLE_STMT, SQLExecDirectW(_stmt, (SQLWCHAR*)(insert_stmt.c_str()), SQL_NTS), ss.str().c_str());

    return Status::OK();
}

Status ODBCScanner::error_status(const std::string& prefix, const std::string& error_msg) {
    std::stringstream msg;
    msg << prefix << " Err: " << error_msg;
    LOG(WARNING) << msg.str();
    return Status::InternalError(msg.str());
}

std::string ODBCScanner::handle_diagnostic_record(SQLHANDLE      hHandle,
                                           SQLSMALLINT    hType,
                                           RETCODE        RetCode) {
    SQLSMALLINT rec = 0;
    SQLINTEGER  error;
    CHAR       message[1000];
    CHAR       state[SQL_SQLSTATE_SIZE+1];

    if (RetCode == SQL_INVALID_HANDLE) {
        return "Invalid handle!";
    }

    std::string diagnostic_msg;

    while (SQLGetDiagRec(hType,
                         hHandle,
                         ++rec,
                         (SQLCHAR *)(state),
                         &error,
                         reinterpret_cast<SQLCHAR *>(message),
                         (SQLSMALLINT)(sizeof(message) / sizeof(WCHAR)),
                         (SQLSMALLINT *)NULL) == SQL_SUCCESS) {
        // Hide data truncated..
        if (wcsncmp(reinterpret_cast<const wchar_t *>(state), L"01004", 5)) {
            boost::format msg_string("%s %s (%d)");
            msg_string % state % message % error;
            diagnostic_msg += msg_string.str();
        }
    }

    return diagnostic_msg;
}

std::string ODBCScanner::build_connect_string(const ODBCScannerParam& param) {
    // different database have different connection string
    // oracle connect string
    if (param.type == TOdbcTableType::ORACLE) {
        boost::format connect_string("Driver=%s;Dbq=//%s:%s/%s;DataBase=%s;Uid=%s;Pwd=%s;charset=%s");
        connect_string % param.drivier % param.host % param.port % param.db % param.db % param.user % param.passwd %
        param.charest;

        return connect_string.str();
    } else if (param.type == TOdbcTableType::MYSQL) {
        boost::format connect_string("Driver=%s;Server=%s;Port=%s;DataBase=%s;Uid=%s;Pwd=%s;charset=%s");
        connect_string % param.drivier % param.host % param.port % param.db % param.user % param.passwd %
        param.charest;
        return connect_string.str();
    }

    return "";
}

}
