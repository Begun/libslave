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

#ifndef __SLAVE_RECORDSET_H_
#define __SLAVE_RECORDSET_H_

#include <boost/any.hpp>
#include <map>
#include <string>

namespace slave
{

// One row in a table. Key -- field name, value - pair of (field type, value)
typedef std::map<std::string, std::pair<std::string, boost::any> > Row;

struct RecordSet
{
    Row m_row, m_old_row;

    std::string tbl_name;
    std::string db_name;

    time_t when;

    enum TypeEvent { Update, Delete, Write, PreInit, PostInit };

    TypeEvent type_event;
	 
    // Root master ID from which this record originated
    unsigned int master_id;
    RecordSet(): master_id(0) {}
};

}// slave

#endif
