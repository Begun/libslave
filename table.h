
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

#define HA_OPTION_PACK_RECORD 1


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

    // Количество полей NULL в таблицы
    unsigned int m_null_fields;

    int db_create_options;

    const std::string m_row_format;

    //некий байт, определяющий количество null_bytes
    int m_db_create_options;

public:

    std::vector<PtrField> fields;

    callback m_callback;

    void call_callback(slave::RecordSet& _rs) {

        // Профайлинг
        stats::incTableCount(full_name);

        // Стата
        stats::setLastFilteredUpdateTime();

        m_callback(_rs);
    }


    unsigned int null_bytes;

    const std::string table_name;
    const std::string database_name;

    std::string full_name;

    Table(const std::string& db_name, const std::string& tbl_name, const std::string& row_format) : 
        m_row_format(row_format), table_name(tbl_name), database_name(db_name), 
        full_name(database_name + "." + table_name)
        {}

    Table() {}

    unsigned int getNullBytes() {

	//оно принимает значение 0 или 1
	unsigned int null_bit_pos= (m_db_create_options & HA_OPTION_PACK_RECORD) ? 0 : 1;
		
	unsigned int null_bytes = (m_null_fields + null_bit_pos + 7) / 8;
	
	return null_bytes;
    }

    void setNullFields(const unsigned int count_null_fields) {
	m_null_fields = count_null_fields;
    }

    void setDbOptions(const int db_options) {
	m_db_create_options = db_options;
    }
};

}

#endif 
