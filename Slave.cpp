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



#include "Slave.h"
#include "SlaveStats.h"

#include "Logging.h"

#include "nanomysql.h"


namespace slave
{

unsigned char *net_store_data(unsigned char *to, const unsigned char *from, unsigned int length)
{
    to = net_store_length_fast(to,length);
    ::memcpy(to,from,length);
    return to+length;
}

unsigned char *net_store_length_fast(unsigned char *pkg, unsigned int length)
{
    unsigned char *packet=(unsigned char*) pkg;
    if (length < 251)
    {
        *packet=(unsigned char) length;
        return packet+1;
    }
    *packet++=252;
    int2store(packet,(unsigned int) length);
    return packet+2;
}







void Slave::init()
{

    LOG_TRACE(log, "Initializing libslave...");

    check_master_version();

    check_master_binlog_format();

    ext_state.loadMasterInfo( m_master_info.master_log_name, m_master_info.master_log_pos);

    LOG_TRACE(log, "Libslave initialized OK");
}


void Slave::close_connection()
{
    ::shutdown(mysql.net.fd, SHUT_RDWR);
    ::close(mysql.net.fd);
}


void Slave::createDatabaseStructure_(table_order_t& tabs, RelayLogInfo& rli) const {

    LOG_TRACE(log, "enter: createDatabaseStructure");

    nanomysql::Connection conn(m_master_info.host.c_str(), m_master_info.user.c_str(),
                               m_master_info.password.c_str(), "", m_master_info.port);
    const collate_map_t collate_map = readCollateMap(conn);


    for (table_order_t::const_iterator it = tabs.begin(); it != tabs.end(); ++ it) {

        LOG_INFO( log, "Creating database structure for: " << it->first << ", Creating table for: " << it->second );
        createTable(rli, it->first, it->second, collate_map, conn);
    }

    LOG_TRACE(log, "exit: createDatabaseStructure");
}



void Slave::createTable(RelayLogInfo& rli,
                        const std::string& db_name, const std::string& tbl_name,
                        const collate_map_t& collate_map, nanomysql::Connection& conn) const {

    LOG_TRACE(log, "enter: createTable " << db_name << " " << tbl_name);

    nanomysql::Connection::result_t res;
    
    conn.query("SHOW FULL COLUMNS FROM " + tbl_name + " IN " + db_name);
    conn.store(res);

    boost::shared_ptr<Table> table(new Table(db_name, tbl_name));


    LOG_DEBUG(log, "Created new Table object: database:" << db_name << " table: " << tbl_name );

    for (nanomysql::Connection::result_t::const_iterator i = res.begin(); i != res.end(); ++i) {

        //row.at(0) - field name
        //row.at(1) - field type
        //row.at(2) - collation
        //row.at(3) - can be null


        std::map<std::string,nanomysql::field>::const_iterator z = i->find("Field");

        if (z == i->end())
            throw std::runtime_error("Slave::create_table(): DESCRIBE query did not return 'Field'");

        std::string name = z->second.data;

        z = i->find("Type");

        if (z == i->end())
            throw std::runtime_error("Slave::create_table(): DESCRIBE query did not return 'Type'");

        std::string type = z->second.data;

        z = i->find("Null");

        if (z == i->end())
            throw std::runtime_error("Slave::create_table(): DESCRIBE query did not return 'Null'");

        std::string extract_field;

        // Extract field type
        for (size_t tmpi = 0; tmpi < type.size(); ++tmpi) {

            if (!((type[tmpi] >= 'a' && type[tmpi] <= 'z') ||
                  (type[tmpi] >= 'A' && type[tmpi] <= 'Z'))) {

                extract_field = type.substr(0, tmpi);
                break;
            }

            if (tmpi == type.size()-1) {
                extract_field = type;
                break;
            }
        }

        if (extract_field.empty())
            throw std::runtime_error("Slave::create_table(): Regexp error, type not found");

        collate_info ci;
        if ("varchar" == extract_field || "char" == extract_field)
        {
            z = i->find("Collation");
            if (z == i->end())
                throw std::runtime_error("Slave::create_table(): DESCRIBE query did not return 'Collation' for field '" + name + "'");
            const std::string collate = z->second.data;
            collate_map_t::const_iterator it = collate_map.find(collate);
            if (collate_map.end() == it)
                throw std::runtime_error("Slave::create_table(): cannot find collate '" + collate + "' from field "
                                         + name + " type " + type + " in collate info map");
            ci = it->second;
            LOG_DEBUG(log, "Created column: name-type: " << name << " - " << type
                      << " Field type: " << extract_field << " Collation: " << ci.name);
        }
        else
            LOG_DEBUG(log, "Created column: name-type: " << name << " - " << type
                      << " Field type: " << extract_field );

        PtrField field;

        if (extract_field == "int")
            field = PtrField(new Field_long(name, type));

        else if (extract_field == "double")
            field = PtrField(new Field_double(name, type));

        else if (extract_field == "float")
            field = PtrField(new Field_float(name, type));

        else if (extract_field == "timestamp")
            field = PtrField(new Field_timestamp(name, type));

        else if (extract_field == "datetime")
            field = PtrField(new Field_datetime(name, type));

        else if (extract_field == "date")
            field = PtrField(new Field_date(name, type));

        else if (extract_field == "year")
            field = PtrField(new Field_year(name, type));

        else if (extract_field == "time")
            field = PtrField(new Field_time(name, type));

        else if (extract_field == "enum")
            field = PtrField(new Field_enum(name, type));

        else if (extract_field == "set")
            field = PtrField(new Field_set(name, type));

        else if (extract_field == "varchar")
            field = PtrField(new Field_varstring(name, type, ci));

        else if (extract_field == "char")
            field = PtrField(new Field_varstring(name, type, ci));

        else if (extract_field == "tinyint")
            field = PtrField(new Field_tiny(name, type));

        else if (extract_field == "smallint")
            field = PtrField(new Field_short(name, type));

        else if (extract_field == "mediumint")
            field = PtrField(new Field_medium(name, type));

        else if (extract_field == "bigint")
            field = PtrField(new Field_longlong(name, type));

        else if (extract_field == "text")
            field = PtrField(new Field_blob(name, type));

        else if (extract_field == "tinytext")
            field = PtrField(new Field_tinyblob(name, type));

        else if (extract_field == "mediumtext")
            field = PtrField(new Field_mediumblob(name, type));

        else if (extract_field == "longtext")
            field = PtrField(new Field_longblob(name, type));

        else if (extract_field == "blob")
            field = PtrField(new Field_blob(name, type));

        else if (extract_field == "tinyblob")
            field = PtrField(new Field_tinyblob(name, type));

        else if (extract_field == "mediumblob")
            field = PtrField(new Field_mediumblob(name, type));

        else if (extract_field == "longblob")
            field = PtrField(new Field_longblob(name, type));

        else {
            LOG_ERROR(log, "createTable: class name don't exist: " << extract_field );
            throw std::runtime_error("class name does not exist: " + extract_field);
        }

        table->fields.push_back(field);

    }


    rli.setTable(tbl_name, db_name, table);

}


struct raii_mysql_connector {

    MYSQL* mysql;
    MasterInfo& m_master_info;
    ExtStateIface &ext_state;

    raii_mysql_connector(MYSQL* m, MasterInfo& mmi, ExtStateIface &state) : mysql(m), m_master_info(mmi), ext_state(state) {

        connect(false);
    }

    ~raii_mysql_connector() {

        end_server(mysql);
        mysql_close(mysql);
    }

    void connect(bool reconnect) {

        LOG_TRACE(log, "enter: connect_to_master");

        ext_state.setConnecting();

        if (reconnect) {
            end_server(mysql);
            mysql_close(mysql);
        }

        if (!(mysql_init(mysql))) {

            throw std::runtime_error("Slave::reconnect() : mysql_init() : could not initialize mysql structure");
        }

        unsigned int timeout = 60;

        mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, &timeout); //(const char*) slave_net_timeout.c_str());

        /* Timeout for reads from server (works only for TCP/IP connections, and only for Windows prior to MySQL 4.1.22).
         * You can this option so that a lost connection can be detected earlier than the TCP/IP
         * Close_Wait_Timeout value of 10 minutes. Added in 4.1.1.
         */
        mysql_options(mysql, MYSQL_OPT_READ_TIMEOUT, &timeout); //(const char*) slave_net_timeout.c_str());

        bool was_error = false;
        while (mysql_real_connect(mysql,
                                  m_master_info.host.c_str(),
                                  m_master_info.user.c_str(),
                                  m_master_info.password.c_str(), 0, m_master_info.port, 0, 0)
               == 0) {


            ext_state.setConnecting();
            if(!was_error) {
                LOG_ERROR(log, "Couldn't connect to mysql master " << m_master_info.host << ":" << m_master_info.port);
                was_error = true;
            }

            LOG_TRACE(log, "try connect to master");
            LOG_TRACE(log, "connect_retry = " << m_master_info.connect_retry << ", reconnect = " << reconnect);

            //
            ::sleep(m_master_info.connect_retry);
        }

        if(was_error)
            LOG_INFO(log, "Successfully connected to " << m_master_info.host << ":" << m_master_info.port);


        mysql->reconnect = 1;

        LOG_TRACE(log, "exit: connect_to_master");
    }
};



void Slave::get_remote_binlog( const boost::function< bool() >& _interruptFlag) {

    try {

        int count_packet = 0;

        generateSlaveId();

        // Moved to Slave member
        // MYSQL mysql;

        raii_mysql_connector __conn(&mysql, m_master_info, ext_state);

        //connect_to_master(false, &mysql);

        register_slave_on_master(&mysql);

connected:

        // Получим позицию бинлога, сохранённую в ext_state ранее, или загрузим её
        // из persistent хранилища. false в случае, если не удалось получить позицию.
        if( !ext_state.getMasterInfo(
                    m_master_info.master_log_name,
                    m_master_info.master_log_pos) ) {
            // Если сохранённой ранее позиции бинлога нет,
            // получаем последнюю версию бинлога и смещение
            std::pair<std::string,unsigned int> row = getLastBinlog();

            m_master_info.master_log_name = row.first;
            m_master_info.master_log_pos = row.second;

            ext_state.setMasterLogNamePos(m_master_info.master_log_name, m_master_info.master_log_pos);
            ext_state.saveMasterInfo();
        }

        LOG_INFO(log, "Starting from binlog_name:binlog_pos : " << m_master_info.master_log_name
                << ":" << m_master_info.master_log_pos );


        request_dump(m_master_info.master_log_name, m_master_info.master_log_pos, &mysql);

        while (!_interruptFlag()) {

            try {


                LOG_TRACE(log, "-- reading event --");

                unsigned long len = read_event(&mysql);

                ext_state.setStateProcessing(true);

                count_packet++;
                LOG_TRACE(log, "Got event with length: " << len << " Packet number: " << count_packet );

                // end of data

                if (len == packet_error || len == packet_end_data) {

                    uint mysql_error_number = mysql_errno(&mysql);

                    switch(mysql_error_number) {
                        case ER_NET_PACKET_TOO_LARGE:
                            LOG_ERROR(log, "Myslave: Log entry on master is longer than max_allowed_packet on "
                                    "slave. If the entry is correct, restart the server with a higher value of "
                                    "max_allowed_packet. max_allowed_packet=" << mysql_error(&mysql) );
                            break;
                        case ER_MASTER_FATAL_ERROR_READING_BINLOG: // Ошибка -- неизвестный бинлог-файл.
                            LOG_ERROR(log, "Myslave: fatal error reading binlog. " <<  mysql_error(&mysql) );
                            break;
                        case 2013: // Обработка ошибки 'Lost connection to MySQL'
                            LOG_WARNING(log, "Myslave: Error from MySQL: " << mysql_error(&mysql) );
                            break;
                        default:
                            LOG_ERROR(log, "Myslave: Error reading packet from server: " << mysql_error(&mysql)
                                    << "; mysql_error: " << mysql_errno(&mysql));
                            break;
                    }

                    __conn.connect(true);

                    goto connected;
                } // len == packet_error

                // Ok event

                if (len == packet_end_data) {
                    continue;
                }

                slave::Basic_event_info event;

                if (!slave::read_log_event((const char*) mysql.net.read_pos + 1,
                            len - 1, 
                            event)) {

                    LOG_TRACE(log, "Skipping unknown event.");
                    continue;
                }

                //

                LOG_TRACE(log, "Event log position: " << event.log_pos );

                if (event.log_pos != 0) {
                    m_master_info.master_log_pos = event.log_pos;
                    ext_state.setLastEventTimePos(event.when, event.log_pos);
                }

                LOG_TRACE(log, "seconds_behind_master: " << (::time(NULL) - event.when) );


                // MySQL5.1.23 binlogs can be read only starting from a XID_EVENT
                // MySQL5.1.23 ev->log_pos -- the binlog offset

                if (event.type == XID_EVENT) {

                    ext_state.setMasterLogNamePos(m_master_info.master_log_name, m_master_info.master_log_pos);

                    LOG_TRACE(log, "Got XID event. Using binlog name:pos: "
                            << m_master_info.master_log_name << ":" << m_master_info.master_log_pos);


                    if (m_xid_callback)
                        m_xid_callback(event.server_id);

                } else  if (event.type == ROTATE_EVENT) {

                    slave::Rotate_event_info rei(event.buf, event.event_len);

                    /*
                     * new_log_ident - new binlog name
                     * pos - position of the starting event
                     */

                    LOG_INFO(log, "Got rotate event.");

                    /* WTF
                     */

                    if (event.when == 0) {

                        //LOG_TRACE(log, "ROTATE_FAKE");
                    }

                    m_master_info.master_log_name = rei.new_log_ident;
                    m_master_info.master_log_pos = rei.pos; // this will always be equal to 4

                    ext_state.setMasterLogNamePos(m_master_info.master_log_name, m_master_info.master_log_pos);

                    LOG_TRACE(log, "ROTATE_EVENT processed OK.");
                }


                if (process_event(event, m_rli, m_master_info.master_log_pos)) {

                    LOG_TRACE(log, "Error in processing event.");
                }



            } catch (const std::exception& _ex ) {

                LOG_ERROR(log, "Met exception in get_remote_binlog cycle. Message: " << _ex.what() );
                usleep(1000*1000);
                continue;

            }

        } //while

        LOG_WARNING(log, "Binlog monitor was stopped. Binlog events are not listened.");

        deregister_slave_on_master(&mysql);
    } catch (const std::exception & e) {
        std::string msg = "[";
        msg += ext_state.getMasterLogName();
        msg += " : ";
        msg += ext_state.getMasterLogPos();
        msg += "] ";
        msg += e.what();
        throw std::runtime_error(msg);
    } catch (...) {
        std::string msg = "[";
        msg += ext_state.getMasterLogName();
        msg += " : ";
        msg += ext_state.getMasterLogPos();
        msg += "] ";
        msg += "Unknown exception";
        throw std::runtime_error(msg);
    }
}

std::map<std::string,std::string> Slave::getRowType(const std::string& db_name,
                                                    const std::set<std::string>& tbl_names) const {

    nanomysql::Connection conn(m_master_info.host.c_str(), m_master_info.user.c_str(),
                               m_master_info.password.c_str(), "", m_master_info.port);

    nanomysql::Connection::result_t res;

    conn.query("SHOW TABLE STATUS FROM " + db_name);
    conn.store(res);

    std::map<std::string,std::string> ret;

    for (nanomysql::Connection::result_t::const_iterator i = res.begin(); i != res.end(); ++i) {

        if (i->size() <= 3) {
            LOG_ERROR(log, "WARNING: Broken SHOW TABLE STATUS FROM " << db_name);
            continue;
        }
        
        //row[0] - the table name
        //row[3] - row_format

        std::map<std::string,nanomysql::field>::const_iterator z = i->find("Name");

        if (z == i->end())
            throw std::runtime_error("Slave::create_table(): SHOW TABLE STATUS query did not return 'Name'");

        std::string name = z->second.data;

        z = i->find("Row_format");

        if (z == i->end())
            throw std::runtime_error("Slave::create_table(): SHOW TABLE STATUS query did not return 'Row_format'");

        std::string format = z->second.data;

        if (tbl_names.count(name) != 0) {

            ret[name] = format;

            LOG_DEBUG(log, name << " row_type = " << format);
        }
    }

    return ret;
}



void Slave::register_slave_on_master(MYSQL* mysql) {

    uchar buf[1024], *pos= buf;

    unsigned int report_host_len=0, report_user_len=0, report_password_len=0;

    const char * report_host = "0.0.0.0";

    const char* report_user = "begun_slave";
    const char* report_password = "begun_slave";
    unsigned int report_port = 0;
    unsigned long rpl_recovery_rank = 0;

    report_host_len= strlen(report_host);
    report_user_len= strlen(report_user);
    report_password_len= strlen(report_password);

    LOG_DEBUG(log, "Register slave on master: m_server_id = " << m_server_id);

    int4store(pos, m_server_id);
    pos+= 4;
    pos= net_store_data(pos, (uchar*)report_host, report_host_len);
    pos= net_store_data(pos, (uchar*)report_user, report_user_len);
    pos= net_store_data(pos, (uchar*)report_password, report_password_len);
    int2store(pos, (unsigned short) report_port);
    pos+= 2;
    int4store(pos, rpl_recovery_rank);
    pos+= 4;

    /* The master will fill in master_id */
    int4store(pos, 0);
    pos+= 4;

    if (simple_command(mysql, COM_REGISTER_SLAVE, buf, (size_t) (pos-buf), 0)) {

        LOG_ERROR(log, "Unable to register slave.");
        throw std::runtime_error("Slave::register_slave_on_master(): Error registring on slave: " +
                                 std::string(mysql_error(mysql)));
    }

    LOG_TRACE(log, "Success registering slave on master");
}

void Slave::deregister_slave_on_master(MYSQL* mysql) {

    LOG_DEBUG(log, "Deregistering slave on master: m_server_id = " << m_server_id << "...");
    // Last '1' means 'no checking', otherwise command can hang
    simple_command(mysql, COM_QUIT, 0, 0, 1);
}

void Slave::check_master_version() {

    nanomysql::Connection conn(m_master_info.host.c_str(), m_master_info.user.c_str(),
                               m_master_info.password.c_str(), "", m_master_info.port);

    nanomysql::Connection::result_t res;

    conn.query("SELECT VERSION()");
    conn.store(res);

    if (res.size() == 1 && res[0].size() == 1) {

        std::string tmp = res[0].begin()->second.data;

        bool ok = true;

        const char* v = tmp.data();

        for (int i = 0; i < 3; ++i) {

            char* e;

            int z = ::strtoul(v, &e, 10);

            /* Mysql version >= 5.1.23 */

            if ((z >= 5 && i == 0) ||
                (z >= 1 && i == 1) ||
                (z >= 23 && i == 2)) {

                //

            }  else {
                ok = false;
                break;
            }

            if (*e == '\0' || e == v) 
                break;

            v = e+1;
        }

        /* */
        if (ok) {
            return;
        }

        throw std::runtime_error("Slave::check_master_version(): got invalid version: " + tmp);
    }

    throw std::runtime_error("Slave::check_master_version(): could not SELECT VERSION()");

}

void Slave::check_master_binlog_format() {

    nanomysql::Connection conn(m_master_info.host.c_str(), m_master_info.user.c_str(),
                               m_master_info.password.c_str(), "", m_master_info.port);

    nanomysql::Connection::result_t res;

    conn.query("SHOW GLOBAL VARIABLES LIKE 'binlog_format'");
    conn.store(res);

    if (res.size() == 1 && res[0].size() == 2) {

        std::map<std::string,nanomysql::field>::const_iterator z = res[0].find("Value");

        if (z == res[0].end())
            throw std::runtime_error("Slave::create_table(): SHOW GLOBAL VARIABLES query did not return 'Value'");

        std::string tmp = z->second.data;

        if (tmp == "ROW") {
            return;

        } else {
            throw std::runtime_error("Slave::check_binlog_format(): got invalid binlog format: " + tmp);
        }
    }


    throw std::runtime_error("Slave::check_binlog_format(): Could not SHOW GLOBAL VARIABLES LIKE 'binlog_format'");
}




// This will check if a QUERY_EVENT holds an "ALTER TABLE ..." string.
namespace
{

static bool checkAlterQuery(const std::string& str)
{
    //RegularExpression regexp("^\\s*ALTER\\s+TABLE)", RegularExpression::RE_CASELESS);

    enum {
        SP0, _A0, _L0, _T0, _E0, _R, SP1, _T1, _A1, _B, _L1, _E1 
    } state = SP0;


    for (std::string::const_iterator i = str.begin(); i != str.end(); ++i) {

        if (state == SP0 && (*i == ' ' || *i == '\t' || *i == '\r' || *i == '\n')) {

        } else if (state == SP0 && (*i == 'a' || *i == 'A')) {
            state = _A0;

        } else if (state == _A0 && (*i == 'l' || *i == 'L')) {
            state = _L0;

        } else if (state == _L0 && (*i == 't' || *i == 'T')) {
            state = _T0;

        } else if (state == _T0 && (*i == 'e' || *i == 'E')) {
            state = _E0;

        } else if (state == _E0 && (*i == 'r' || *i == 'R')) {
            state = _R;

        } else if (state == _R && (*i == ' ' || *i == '\t' || *i == '\r' || *i == '\n')) {
            state = SP1;

        } else if (state == SP1 && (*i == ' ' || *i == '\t' || *i == '\r' || *i == '\n')) {
            
        } else if (state == SP1 && (*i == 't' || *i == 'T')) {
            state = _T1;

        } else if (state == _T1 && (*i == 'a' || *i == 'A')) {
            state = _A1;

        } else if (state == _A1 && (*i == 'b' || *i == 'B')) {
            state = _B;

        } else if (state == _B && (*i == 'l' || *i == 'L')) {
            state = _L1;

        } else if (state == _L1 && (*i == 'e' || *i == 'E')) {
            state = _E1;

        } else if (state == _E1) {
            return true;

        } else {
            return false;
        }
    }

    return false;
}

bool checkCreateQuery(const std::string& str)
{
    if (0 == ::strncasecmp("create table ", str.c_str(), 13))
        return true;
    return false;
}

bool checkDropTableQuery(const std::string& str)
{
    if (0 == ::strncasecmp("drop table ", str.c_str(), 11))
    {
        return true;
    }
    return false;
}
}

int Slave::process_event(const slave::Basic_event_info& bei, RelayLogInfo &m_rli, unsigned long long pos)
{


    if (bei.when < 0 &&
        bei.type != FORMAT_DESCRIPTION_EVENT)
        return 0;

    switch (bei.type) {

    case QUERY_EVENT:
    {
        // Check for an ALTER TABLE 

        slave::Query_event_info qei(bei.buf, bei.event_len);

        LOG_TRACE(log, "Received QUERY_EVENT: " << qei.query);

        if (checkAlterQuery(qei.query) || checkCreateQuery(qei.query) || checkDropTableQuery(qei.query)) {

            LOG_DEBUG(log, "Rebuilding database structure.");
            createDatabaseStructure();
        }
        break;
    }

    case TABLE_MAP_EVENT:
    {
        LOG_TRACE(log, "Got TABLE_MAP_EVENT.");

        slave::Table_map_event_info tmi(bei.buf, bei.event_len);

        m_rli.setTableName(tmi.m_table_id, tmi.m_tblnam, tmi.m_dbnam);

        break;
    }

    case WRITE_ROWS_EVENT:
    case UPDATE_ROWS_EVENT:
    case DELETE_ROWS_EVENT:
    {
        LOG_TRACE(log, "Got " << (bei.type == WRITE_ROWS_EVENT ? "WRITE" :
                                  bei.type == DELETE_ROWS_EVENT ? "DELETE" :
                                  "UPDATE") << "_ROWS_EVENT");

        Row_event_info roi(bei.buf, bei.event_len, (bei.type == UPDATE_ROWS_EVENT));

        apply_row_event(m_rli, bei, roi, ext_state);

        break;
    }
   
    default:
        break;
    }

    return 0;
}

void Slave::request_dump(const std::string& logname, unsigned long start_position, MYSQL* mysql) {

    uchar buf[128];

    /*
    COM_BINLOG_DUMP accepts only 4 bytes for the position, so we are forced to
    cast to uint32.
    */

    //
    //start_position = 4;

    int binlog_flags = 0;
    int4store(buf, (uint32)start_position);
    int2store(buf + BIN_LOG_HEADER_SIZE, binlog_flags);

    uint logname_len = logname.size();
    int4store(buf + 6, m_server_id);

    memcpy(buf + 10, logname.data(), logname_len);

    if (simple_command(mysql, COM_BINLOG_DUMP, buf, logname_len + 10, 1)) {

        LOG_ERROR(log, "Error sending COM_BINLOG_DUMP");
        throw std::runtime_error("Error in sending COM_BINLOG_DUMP");
    }
}


ulong Slave::read_event(MYSQL* mysql)
{

    ulong len;
    ext_state.setStateProcessing(false);

    len = cli_safe_read(mysql);

    if (len == packet_error) {
        LOG_ERROR(log, "Myslave:Error reading packet from server: " << mysql_error(mysql)
                  << "; mysql_error: " << mysql_errno(mysql));

        return packet_error;
    }

    // check for end-of-data
    if (len < 8 && mysql->net.read_pos[0] == 254) {

        LOG_ERROR(log, "read_event(): end of data\n");
        return packet_end_data;
    }

    return len;
}


void Slave::generateSlaveId()
{

    std::set<unsigned int> server_ids;

    nanomysql::Connection conn(m_master_info.host.c_str(), m_master_info.user.c_str(),
                               m_master_info.password.c_str(), "", m_master_info.port);

    nanomysql::Connection::result_t res;

    conn.query("SHOW SLAVE HOSTS");
    conn.store(res);

    for (nanomysql::Connection::result_t::const_iterator i = res.begin(); i != res.end(); ++i) {

        //row[0] - server_id

        std::map<std::string,nanomysql::field>::const_iterator z = i->find("Server_id");
        
        if (z == i->end())
            throw std::runtime_error("Slave::create_table(): SHOW SLAVE HOSTS query did not return 'Server_id'");

        server_ids.insert(::strtoul(z->second.data.c_str(), NULL, 10));
    }

    unsigned int serveroid = ::time(NULL);
    serveroid ^= (::getpid() << 16);

    while (1) {

        if (serveroid < 0) serveroid = -serveroid;

        if (server_ids.count(serveroid) != 0) {
            serveroid++;
        } else {
            break;
        }
    }

    m_server_id = serveroid;

    LOG_DEBUG(log, "Generated m_server_id = " << m_server_id);
}




std::pair<std::string,unsigned int> Slave::getLastBinlog()
{


    nanomysql::Connection conn(m_master_info.host.c_str(), m_master_info.user.c_str(),
                               m_master_info.password.c_str(), "", m_master_info.port);

    nanomysql::Connection::result_t res;

    conn.query("SHOW MASTER STATUS");
    conn.store(res);

    if (res.size() == 1 && res[0].size() == 4) {

        std::map<std::string,nanomysql::field>::const_iterator z = res[0].find("File");
        
        if (z == res[0].end())
            throw std::runtime_error("Slave::create_table(): SHOW SLAVE HOSTS query did not return 'File'");

        std::string file = z->second.data;

        z = res[0].find("Position");
        
        if (z == res[0].end())
            throw std::runtime_error("Slave::create_table(): SHOW SLAVE HOSTS query did not return 'Position'");

        std::string pos = z->second.data;

        return std::make_pair(file, ::strtoul(pos.c_str(), NULL, 10));
    }

    throw std::runtime_error("Slave::getLastBinLog(): Could not SHOW MASTER STATUS");
}




} //namespace slave

