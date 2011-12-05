
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

#ifndef __SLAVE_FIELD_H_
#define __SLAVE_FIELD_H_

#include <string>
#include <vector>
#include <list>

#include <boost/any.hpp>

#include "collate.h"

//конфликт с макросом, опредленном в MYSQL
#ifdef test
#undef test
#endif /* test */

namespace slave
{


class Field {

public:

    unsigned int field_length;

    const std::string field_type;
    const std::string field_name;

    boost::any field_data;

    virtual const char* unpack(const char *from) = 0;

    Field(const std::string& field_name_arg, const std::string& type) :
        field_type(type),
        field_name(field_name_arg),
        is_bad(false)
        {}

    virtual ~Field() {}

    virtual unsigned int pack_length() const {
        return (unsigned int) field_length;
    }

    const std::string getFieldName() {
        return field_name;
    }


protected:

    //const std::string table_name;

public:
    // HACK!
    // Борьба с багой row-level репликации mysql.
    bool is_bad;

};



/*
 *
 */



class Field_num: public Field {
public:
    Field_num(const std::string& field_name_arg, const std::string& type);
};


class Field_str: public Field {
public:
    Field_str(const std::string& field_name_arg, const std::string& type);
};

class Field_longstr: public Field_str {
public:
    Field_longstr(const std::string& field_name_arg, const std::string& type);

    const char* unpack(const char* from);

protected:
    unsigned int length_row;
};


class Field_real: public Field_num {
public:
    Field_real(const std::string& field_name_arg, const std::string& type);
};

class Field_decimal: public Field_real {
public:
    Field_decimal(const std::string& field_name_arg, const std::string& type);
};

class Field_new_decimal: public Field_num {
};

class Field_tiny: public Field_num {
    unsigned int pack_length() const { return 1; }
public:
    Field_tiny(const std::string& field_name_arg, const std::string& type);
    const char* unpack(const char* from);
};

class Field_short: public Field_num {
    unsigned int pack_length() const { return 2; }
public:
    Field_short(const std::string& field_name_arg, const std::string& type);

    const char* unpack(const char* from);
};

class Field_medium: public Field_num {
    unsigned int pack_length() const { return 3; }
public:
    Field_medium(const std::string& field_name_arg, const std::string& type);

    const char* unpack(const char* from);
};

class Field_long: public Field_num {
    unsigned int pack_length() const { return 4; }
public:
    Field_long(const std::string& field_name_arg, const std::string& type);

    const char* unpack(const char* from);
};

class Field_longlong: public Field_num {
    unsigned int pack_length() const { return 8; }
public:
    Field_longlong(const std::string& field_name_arg, const std::string& type);

    const char* unpack(const char* from);
};

class Field_float: public Field_real {
    unsigned int pack_length() const { return sizeof(float); }
public:
    Field_float(const std::string& field_name_arg, const std::string& type);

    const char* unpack(const char* from);
};

class Field_double: public Field_real {
    unsigned int pack_length() const { return sizeof(double); }
public:
    Field_double(const std::string& field_name_arg, const std::string& type);

    const char* unpack(const char* from);
};

class Field_null: public Field_str {
    unsigned int pack_length() const { return 0; }
};

class Field_timestamp: public Field_str {
    unsigned int pack_length() const { return 4; }
public:
    Field_timestamp(const std::string& field_name_arg, const std::string& type);

    const char* unpack(const char* from);
};

class Field_year: public Field_tiny {
public:
    Field_year(const std::string& field_name_arg, const std::string& type);
};

class Field_date: public Field_str {
    unsigned int pack_length() const { return 3; }
public:
    Field_date(const std::string& field_name_arg, const std::string& type);

    const char* unpack(const char* from);
};

class Field_newdate: public Field_str {
    unsigned int pack_length() const { return 3; }
};

class Field_time: public Field_str {
    unsigned int pack_length() const { return 3; }
public:

    Field_time(const std::string& field_name_arg, const std::string& type);

    const char* unpack(const char* from);
};

class Field_datetime: public Field_str {
    unsigned int pack_length() const { return 8; }
public:
    Field_datetime(const std::string& field_name_arg, const std::string& type);

    const char* unpack(const char* from);
};

class Field_string: public Field_longstr {
public:
    Field_string(const std::string& field_name_arg, const std::string& type);
};

class Field_varstring: public Field_longstr {

    /* Количество байт для указания длины (1 или 2 байта)  */
    unsigned int length_bytes;

    unsigned int pack_length() const { return (unsigned int) field_length+length_bytes; }

public:
    Field_varstring(const std::string& field_name_arg, const std::string& type,
            const collate_info& collate);

    const char* unpack(const char* from);
};

class Field_blob: public Field_longstr {
    unsigned int get_length(const char *ptr);
public:
    Field_blob(const std::string& field_name_arg, const std::string& type);

    const char* unpack(const char* from);

protected:
    /* Количество байт, хранящих размер запакованных данных */
    unsigned int packlength;
};

class Field_tinyblob: public Field_blob {
public:
    Field_tinyblob(const std::string& field_name_arg, const std::string& type);
};

class Field_mediumblob: public Field_blob {
public:
    Field_mediumblob(const std::string& field_name_arg, const std::string& type);
};

class Field_longblob: public Field_blob {
public:
    Field_longblob(const std::string& field_name_arg, const std::string& type);
};

class Field_geom: public Field_blob { };

class Field_enum: public Field_str {

    unsigned int pack_length() const {
        return (unsigned int)(count_elements < 255) ? 1 : 2;
    }

public:
    Field_enum(const std::string& field_name_arg, const std::string& type);


    const char* unpack(const char* from);

protected:
    unsigned int packlength;

    /* Количество перечисляемых элементов */
    unsigned short count_elements;
};

class Field_set: public Field_enum {

    unsigned int pack_length() const {
        return (unsigned int) ((count_elements + 7)/8);
    }

public:
    Field_set(const std::string& field_name_arg, const std::string& type);

    const char* unpack(const char* from);
};

}

#endif
