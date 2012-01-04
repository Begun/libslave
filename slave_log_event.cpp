
/* Copyright 2011 ZAO "Begun".
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library.  If not, see <http://www.gnu.org/licenses/>.
*/


#include <string>
#include <vector>
#include <map>
#include <set>

#include <mysql/my_global.h>
#undef min
#undef max

#include <mysql/mysql.h>

#include "relayloginfo.h"
#include "slave_log_event.h"

#include "SlaveStats.h"
#include "Logging.h"




namespace slave {



//---------------------------------------------------------------------------------------

void Basic_event_info::parse(const char* _buf, unsigned int _event_len) {

    buf = _buf;
    event_len = _event_len;

    if (event_len < LOG_POS_OFFSET + 4) {
        LOG_ERROR(log, "Sanity check failed: " << event_len << " " << LOG_POS_OFFSET + 4);
        ::abort();
    }

    type = (slave::Log_event_type)buf[EVENT_TYPE_OFFSET];

    when = uint4korr(buf);
    server_id = uint4korr(buf + SERVER_ID_OFFSET);
    log_pos = uint4korr(buf + LOG_POS_OFFSET);
}

Rotate_event_info::Rotate_event_info(const char* buf, unsigned int event_len) {

    if (event_len < LOG_EVENT_HEADER_LEN + ROTATE_HEADER_LEN) {
        LOG_ERROR(log, "Sanity check failed: " << event_len << " " << LOG_EVENT_HEADER_LEN + ROTATE_HEADER_LEN);
        ::abort();
    }

    pos = uint8korr(buf + LOG_EVENT_HEADER_LEN + R_POS_OFFSET);
    ident_len = (event_len - LOG_EVENT_HEADER_LEN - ROTATE_HEADER_LEN);
    new_log_ident.assign(buf + LOG_EVENT_HEADER_LEN + ROTATE_HEADER_LEN, ident_len);
}


Query_event_info::Query_event_info(const char* buf, unsigned int event_len) {

    if (event_len < LOG_EVENT_HEADER_LEN + QUERY_HEADER_LEN) {
        LOG_ERROR(log, "Sanity check failed: " << event_len << " " << LOG_EVENT_HEADER_LEN + QUERY_HEADER_LEN);
        ::abort();
    }

    unsigned int db_len = (unsigned int)buf[LOG_EVENT_HEADER_LEN + Q_DB_LEN_OFFSET];

    unsigned int status_vars_len = uint2korr(buf + LOG_EVENT_HEADER_LEN + Q_STATUS_VARS_LEN_OFFSET);

    size_t data_len = event_len - (LOG_EVENT_HEADER_LEN + QUERY_HEADER_LEN) - status_vars_len;

    query.assign(buf + LOG_EVENT_HEADER_LEN + QUERY_HEADER_LEN + status_vars_len + db_len + 1,
                 data_len - db_len - 1);
}


Table_map_event_info::Table_map_event_info(const char* buf, unsigned int event_len) {

    if (event_len < LOG_EVENT_HEADER_LEN + TABLE_MAP_HEADER_LEN + 2) {
        LOG_ERROR(log, "Sanity check failed: " << event_len << " " << LOG_EVENT_HEADER_LEN + TABLE_MAP_HEADER_LEN + 2);
        ::abort();
    }

    m_table_id = uint6korr(buf + LOG_EVENT_HEADER_LEN + TM_MAPID_OFFSET);

    unsigned char* p_dblen = (unsigned char*)(buf + LOG_EVENT_HEADER_LEN + TABLE_MAP_HEADER_LEN);
    size_t dblen = *(p_dblen);

    m_dbnam.assign((const char*)(p_dblen + 1), dblen);

    unsigned char* p_tblen = p_dblen + dblen + 2;
    size_t tblen = *(p_tblen);

    m_tblnam.assign((const char*)(p_tblen + 1), tblen);
}

Row_event_info::Row_event_info(const char* buf, unsigned int event_len, bool do_update) {

    if (event_len < LOG_EVENT_HEADER_LEN + ROWS_HEADER_LEN + 2) {
        LOG_ERROR(log, "Sanity check failed: " << event_len << " " << LOG_EVENT_HEADER_LEN + ROWS_HEADER_LEN + 2);
        ::abort();
    }

    has_after_image = do_update;

    m_table_id = uint6korr(buf + LOG_EVENT_HEADER_LEN + RW_MAPID_OFFSET);

    unsigned char* start = (unsigned char*)(buf + LOG_EVENT_HEADER_LEN + ROWS_HEADER_LEN);

    m_width = net_field_length(&start);

    m_cols.assign(start, start + ((m_width + 7) / 8));

    start += m_cols.size();

    if (do_update) {

        m_cols_ai.assign(start, start + m_cols.size());
        start += m_cols_ai.size();
    }

    m_rows_buf = start;
    m_rows_end = start + (event_len - ((char*)start - buf));
}

/////////////////////////


inline void check_format_description_postlen(unsigned char* b, slave::Log_event_type type, unsigned char len) {

    if (b[(int)type - 1] != len) {

        LOG_ERROR(log, "Invalid Format_description event: event type " << (int)type
                  << " len: " << (int)b[(int)type - 1] << " != " << (int)len);

        ::abort();
    }
}


inline void check_format_description(const char* buf, unsigned int event_len) {

    buf += LOG_EVENT_MINIMAL_HEADER_LEN;

    uint16_t binlog_version;
    memcpy(&binlog_version, buf + ST_BINLOG_VER_OFFSET, ST_BINLOG_VER_LEN);
    if (4 != binlog_version)
    {
        LOG_ERROR(log, "Invalid binlog version: " << binlog_version << " != 4");
        ::abort();
    }

    size_t common_header_len = (unsigned char)(buf[ST_COMMON_HEADER_LEN_OFFSET]);

    if (common_header_len != LOG_EVENT_HEADER_LEN) {

        LOG_ERROR(log, "Invalid Format_description event: common_header_len " << common_header_len
                  << " != " << LOG_EVENT_HEADER_LEN);
        ::abort();
    }

    // Check that binlog contains different event types not more than we know
    // If less - it's ok, backward compatibility
    const size_t number_of_event_types =
        event_len - (LOG_EVENT_MINIMAL_HEADER_LEN + ST_COMMON_HEADER_LEN_OFFSET + 1);

    if (number_of_event_types > LOG_EVENT_TYPES)
    {
        LOG_ERROR(log, "Invalid Format_description event: number_of_event_types " << number_of_event_types
                  << " > " << LOG_EVENT_TYPES);
        ::abort();
    }

    unsigned char event_lens[LOG_EVENT_TYPES] = { 0, };

    ::memcpy(&event_lens[0], (unsigned char*)(buf + ST_COMMON_HEADER_LEN_OFFSET + 1), LOG_EVENT_TYPES);

    check_format_description_postlen(event_lens, XID_EVENT, 0);
    check_format_description_postlen(event_lens, QUERY_EVENT, QUERY_HEADER_LEN);
    check_format_description_postlen(event_lens, ROTATE_EVENT, ROTATE_HEADER_LEN);
    check_format_description_postlen(event_lens, FORMAT_DESCRIPTION_EVENT, START_V3_HEADER_LEN + 1 + number_of_event_types);
    check_format_description_postlen(event_lens, TABLE_MAP_EVENT, TABLE_MAP_HEADER_LEN);
    check_format_description_postlen(event_lens, WRITE_ROWS_EVENT, ROWS_HEADER_LEN);
    check_format_description_postlen(event_lens, UPDATE_ROWS_EVENT, ROWS_HEADER_LEN);
    check_format_description_postlen(event_lens, DELETE_ROWS_EVENT, ROWS_HEADER_LEN);

}


bool read_log_event(const char* buf, uint event_len, Basic_event_info& bei)

{

    bei.parse(buf, event_len);


    /* Check the integrity */

    if (event_len < EVENT_LEN_OFFSET ||
        bei.type >= ENUM_END_EVENT ||
        (uint) event_len != uint4korr(buf+EVENT_LEN_OFFSET))
    {
        LOG_ERROR(log, "Sanity check failed: " << event_len);
        ::abort();
    }

    switch (bei.type) {

    case FORMAT_DESCRIPTION_EVENT:

        check_format_description(buf, event_len);

        return true;
        break;

    case QUERY_EVENT:
    case ROTATE_EVENT:
    case XID_EVENT:
    case WRITE_ROWS_EVENT:
    case UPDATE_ROWS_EVENT:
    case DELETE_ROWS_EVENT:
    case TABLE_MAP_EVENT:
        return true;
        break;

    case LOAD_EVENT:
    case NEW_LOAD_EVENT:
    case SLAVE_EVENT: /* can never happen (unused event) */
    case CREATE_FILE_EVENT:
    case APPEND_BLOCK_EVENT:
    case DELETE_FILE_EVENT:
    case EXEC_LOAD_EVENT:
    case START_EVENT_V3: /* this is sent only by MySQL <=4.x */
    case STOP_EVENT:
    case INTVAR_EVENT:
    case RAND_EVENT:
    case USER_VAR_EVENT:
    case PRE_GA_WRITE_ROWS_EVENT:
    case PRE_GA_UPDATE_ROWS_EVENT:
    case PRE_GA_DELETE_ROWS_EVENT:
    case BEGIN_LOAD_QUERY_EVENT:
    case EXECUTE_LOAD_QUERY_EVENT:
    case INCIDENT_EVENT:
    case HEARTBEAT_LOG_EVENT:
        return false;
        break;

    default:
        LOG_ERROR( log, "Unknown event code: " << (int) bei.type);
        return false;
        break;
    }

    //
    //

    if ( event_len > 1024*1024*3 ) { // >3 Mb
        LOG_ERROR(log, "Warning: event size > 3MB! Maybe a corrupted event!");
    }

    return true;
}



/*
 *
 */


size_t n_set_bits(const std::vector<unsigned char>& b, unsigned int count) {

    size_t ret = 0;

    for (unsigned int i = 0; i < count; ++i) {

        if (b[i / 8] & (1 << (i & 7)))
            ret++;
    }

    return ret;
}


unsigned char* unpack_row(boost::shared_ptr<slave::Table> table,
                          slave::Row& _row,
                          unsigned int colcnt,
                          unsigned char* row,
                          const std::vector<unsigned char>& cols,
                          const std::vector<unsigned char>& cols_ai)
{

    LOG_TRACE(log, "Unpacking row: " << table->fields.size() << "," << colcnt << "," << cols.size()
              << "," << cols_ai.size());

    if (colcnt != table->fields.size()) {
        LOG_ERROR(log, "Field count mismatch in unpacking row for "
                  << table->full_name << ": " << colcnt << " != " << table->fields.size());
        return NULL;
    }


    //указатель на начало данных, пропускаем master_null_bytes

    size_t master_null_byte_count = (n_set_bits(cols, colcnt) + 7) / 8;

    unsigned char* ptr = row + master_null_byte_count;

    //
    unsigned char* null_ptr = row;
    unsigned int null_mask = 1U;
    unsigned char null_bits = *null_ptr++;

    int field_count = table->fields.size();

    for (int i = 0; i < field_count; i++) {

        slave::PtrField field = table->fields[i];

        if (!(cols[i / 8] & (1 << (i & 7)))) {

            LOG_TRACE(log, "field " << field->getFieldName() << " is not in column list.");
            continue;
        }

        if (cols_ai.size() && !(cols_ai[i / 8] & (1 << (i & 7)))) {

            LOG_TRACE(log, "field " << field->getFieldName() << " is not in the update after-image.");
            continue;
        }

        if ((null_mask & 0xFF) == 0) {
            null_mask = 1U;
            null_bits = *null_ptr++;

            LOG_TRACE(log, "null mask reset.");
        }

        if (null_bits & null_mask) {

            LOG_TRACE(log, "set_null found");

        } else {

            // We only unpack the field if it was non-null

            ptr = (unsigned char*)field->unpack((const char*)ptr);

            _row[field->getFieldName()] = std::make_pair(field->field_type, field->field_data);
        }

        null_mask <<= 1;

        LOG_TRACE(log, "field: " << field->getFieldName());

    }

    return ptr;
}


unsigned char* do_writedelete_row(boost::shared_ptr<slave::Table> table,
                                  const Basic_event_info& bei,
                                  const Row_event_info& roi,
                                  unsigned char* row_start) {


    slave::RecordSet _record_set;

    unsigned char* t = unpack_row(table, _record_set.m_row, roi.m_width, row_start, roi.m_cols, roi.m_cols_ai);

    if (t == NULL) {
        return NULL;
    }

    _record_set.when = bei.when;
    _record_set.tbl_name = table->table_name;
    _record_set.db_name = table->database_name;
    _record_set.type_event = (bei.type == WRITE_ROWS_EVENT ? slave::RecordSet::Write : slave::RecordSet::Delete);
    _record_set.master_id = bei.server_id;

    table->call_callback(_record_set);

    return t;
}

unsigned char* do_update_row(boost::shared_ptr<slave::Table> table,
                             const Basic_event_info& bei,
                             const Row_event_info& roi,
                             unsigned char* row_start) {

    slave::RecordSet _record_set;

    unsigned char* t = unpack_row(table, _record_set.m_old_row, roi.m_width, row_start, roi.m_cols, roi.m_cols_ai);

    if (t == NULL) {
        return NULL;
    }

    t = unpack_row(table, _record_set.m_row, roi.m_width, t, roi.m_cols, roi.m_cols_ai);

    if (t == NULL) {
        return NULL;
    }

    _record_set.when = bei.when;
    _record_set.tbl_name = table->table_name;
    _record_set.db_name = table->database_name;
    _record_set.type_event = slave::RecordSet::Update;
    _record_set.master_id = bei.server_id;

    table->call_callback(_record_set);

    return t;
}


void apply_row_event(slave::RelayLogInfo& rli, const Basic_event_info& bei, const Row_event_info& roi) {


    std::pair<std::string,std::string> key = rli.getTableNameById(roi.m_table_id);

    LOG_DEBUG(log, "applyRowEvent(): " << roi.m_table_id << " " << key.first << "." << key.second);

    boost::shared_ptr<slave::Table> table = rli.getTable(key);

    if (table) {

        LOG_DEBUG(log, "Table " << table->database_name << "." << table->table_name << " has callback.");


        unsigned char* row_start = roi.m_rows_buf;

        while (row_start < roi.m_rows_end &&
               row_start != NULL) {

            if (bei.type == UPDATE_ROWS_EVENT) {

                row_start = do_update_row(table, bei, roi, row_start);

            } else {
                row_start = do_writedelete_row(table, bei, roi, row_start);
            }
        }
    }
}



} // namespace

