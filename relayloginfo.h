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

#ifndef __SLAVE_RELAYLOGINFO_H_
#define __SLAVE_RELAYLOGINFO_H_

#include "table.h"

#include <boost/shared_ptr.hpp>

#include <map>
#include <string>



namespace slave {


typedef boost::shared_ptr<Table> PtrTable;

class RelayLogInfo {

public:

    typedef std::map<unsigned long, std::pair<std::string, std::string> > id_to_name_t;
    id_to_name_t m_map_table_name;

    typedef std::map<std::pair<std::string, std::string>, PtrTable> name_to_table_t;
    name_to_table_t m_table_map;


    void clear() {
        m_map_table_name.clear();
        m_table_map.clear();
    }


    void setTableName(unsigned long table_id, const std::string& table_name, const std::string& db_name) {
        m_map_table_name[table_id] = std::make_pair(db_name, table_name);
    }

    const std::pair<std::string,std::string> getTableNameById(int table_id) {

        id_to_name_t::const_iterator p = m_map_table_name.find(table_id);

        if (p != m_map_table_name.end()) {
            return p->second;
        } else {
            return std::make_pair(std::string(), std::string());
        }
    }

    boost::shared_ptr<Table> getTable(const std::pair<std::string, std::string>& key) {
        name_to_table_t::iterator p = m_table_map.find(key);

        if (p != m_table_map.end()) {
            return p->second;

        } else {
            return boost::shared_ptr<Table>();
        }
    }

    void setTable(const std::string& table_name, const std::string& db_name, PtrTable table) {

        m_table_map[std::make_pair(db_name, table_name)] = table;
    }

};
}
#endif
