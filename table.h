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

#ifndef __SLAVE_TABLE_H_
#define __SLAVE_TABLE_H_


#include <string>
#include <vector>
#include <map>

#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>

#include "field.h"
#include "recordset.h"
#include "SlaveStats.h"


namespace slave
{

typedef boost::shared_ptr<Field> PtrField;
typedef boost::function<void (RecordSet&)> callback;


class Table {

public:

    std::vector<PtrField> fields;

    callback m_callback;

    void call_callback(slave::RecordSet& _rs, ExtStateIface &ext_state) {

        // Some stats
        ext_state.incTableCount(full_name);
        ext_state.setLastFilteredUpdateTime();

        m_callback(_rs);
    }


    const std::string table_name;
    const std::string database_name;

    std::string full_name;

    Table(const std::string& db_name, const std::string& tbl_name) :
        table_name(tbl_name), database_name(db_name),
        full_name(database_name + "." + table_name)
        {}

    Table() {}

};

}

#endif
