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


#ifndef __SLAVE_SLAVE_LOG_EVENT_H
#define __SLAVE_SLAVE_LOG_EVENT_H


#include "relayloginfo.h"


namespace slave {



/*
 *
 */

enum Log_event_type
{

  UNKNOWN_EVENT= 0,
  START_EVENT_V3= 1,
  QUERY_EVENT= 2,
  STOP_EVENT= 3,
  ROTATE_EVENT= 4,
  INTVAR_EVENT= 5,
  LOAD_EVENT= 6,
  SLAVE_EVENT= 7,
  CREATE_FILE_EVENT= 8,
  APPEND_BLOCK_EVENT= 9,
  EXEC_LOAD_EVENT= 10,
  DELETE_FILE_EVENT= 11,
  NEW_LOAD_EVENT= 12,
  RAND_EVENT= 13,
  USER_VAR_EVENT= 14,
  FORMAT_DESCRIPTION_EVENT= 15,
  XID_EVENT= 16,
  BEGIN_LOAD_QUERY_EVENT= 17,
  EXECUTE_LOAD_QUERY_EVENT= 18,

  TABLE_MAP_EVENT = 19,

  PRE_GA_WRITE_ROWS_EVENT = 20,
  PRE_GA_UPDATE_ROWS_EVENT = 21,
  PRE_GA_DELETE_ROWS_EVENT = 22,

  WRITE_ROWS_EVENT = 23,
  UPDATE_ROWS_EVENT = 24,
  DELETE_ROWS_EVENT = 25,

  INCIDENT_EVENT = 26,

  HEARTBEAT_LOG_EVENT= 27,

  ENUM_END_EVENT
};

#define LOG_EVENT_TYPES (ENUM_END_EVENT-1)


#define EVENT_TYPE_OFFSET    4
#define SERVER_ID_OFFSET     5
#define EVENT_LEN_OFFSET     9
#define LOG_POS_OFFSET       13

#define LOG_EVENT_HEADER_LEN 19

#define ROTATE_HEADER_LEN    8
#define R_POS_OFFSET       0

#define QUERY_HEADER_MINIMAL_LEN     (4 + 4 + 1 + 2)
#define QUERY_HEADER_LEN     (QUERY_HEADER_MINIMAL_LEN + 2)
#define Q_STATUS_VARS_LEN_OFFSET 11
#define Q_DB_LEN_OFFSET             8

#define TABLE_MAP_HEADER_LEN   8
#define TM_MAPID_OFFSET    0

#define ROWS_HEADER_LEN        8
#define RW_MAPID_OFFSET    0
#define ROWS_HEADER_LEN        8

#define LOG_EVENT_MINIMAL_HEADER_LEN 19

#define ST_BINLOG_VER_LEN           2
#define ST_BINLOG_VER_OFFSET        0
#define ST_SERVER_VER_LEN 50
#define ST_SERVER_VER_OFFSET        (ST_BINLOG_VER_OFFSET + 2)
#define ST_CREATED_OFFSET     (ST_SERVER_VER_OFFSET + ST_SERVER_VER_LEN)
#define ST_COMMON_HEADER_LEN_OFFSET (ST_CREATED_OFFSET + 4)

#define START_V3_HEADER_LEN     (2 + ST_SERVER_VER_LEN + 4)


//-----------------------------------------------------------------------------------------

struct Basic_event_info {

    slave::Log_event_type type;
    size_t log_pos;
    time_t when;
    unsigned int server_id;

    const char* buf;
    unsigned int event_len;

    Basic_event_info() : type(UNKNOWN_EVENT), log_pos(0), when(0), server_id(0), buf(NULL), event_len(0)
        {}

    void parse(const char* b, unsigned int el);
};

struct Rotate_event_info {

    unsigned int ident_len;
    std::string new_log_ident;
    size_t pos;

    Rotate_event_info(const char* buf, unsigned int event_len);
};

struct Query_event_info {

    std::string query;

    Query_event_info(const char* buf, unsigned int event_len);
};

struct Table_map_event_info {

    unsigned long m_table_id;
    std::string m_tblnam;
    std::string m_dbnam;

    Table_map_event_info(const char* buf, unsigned int event_len);
};

struct Row_event_info {

    unsigned long m_width;
    unsigned long m_table_id;

    std::vector<unsigned char> m_cols;
    std::vector<unsigned char> m_cols_ai;

    unsigned char* m_rows_buf;
    unsigned char* m_rows_end;

    bool has_after_image;

    Row_event_info(const char* buf, unsigned int event_len, bool do_update);
};


bool read_log_event(const char* buf, unsigned int event_len, Basic_event_info& info);

void apply_row_event(slave::RelayLogInfo& rli, const Basic_event_info& bei, const Row_event_info& roi, ExtStateIface &ext_state);


//------------------------------------------------------------------------------------------







}; //namespace


#endif
