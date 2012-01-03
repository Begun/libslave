
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


#include <cstdio>
#include <vector>
#include <stdexcept>

#include <mysql/my_global.h>
#undef min
#undef max

#include <mysql/m_string.h>

#include "field.h"

#include "Logging.h"




namespace slave
{



Field_num::Field_num(const std::string& field_name_arg, const std::string& type):
    Field(field_name_arg, type) {}

Field_tiny::Field_tiny(const std::string& field_name_arg, const std::string& type):
    Field_num(field_name_arg, type) {}

const char* Field_tiny::unpack(const char* from) {

    char tmp = *((char*)(from));
    field_data = tmp;

    LOG_TRACE(log, "  tiny: " << (int)(tmp) << " // " << pack_length());

    return from + pack_length();
}


Field_short::Field_short(const std::string& field_name_arg, const std::string& type):
    Field_num(field_name_arg, type) {}

const char* Field_short::unpack(const char* from) {

    uint16 tmp = uint2korr(from);
    field_data = tmp;

    LOG_TRACE(log, "  short: " << tmp << " // " << pack_length());

    return from + pack_length();
}

Field_medium::Field_medium(const std::string& field_name_arg, const std::string& type):
    Field_num(field_name_arg, type) {}

const char* Field_medium::unpack(const char* from) {

    uint32 tmp = uint3korr(from);
    field_data = tmp;

    LOG_TRACE(log, "  medium: " << tmp << " // " << pack_length());

    return from + pack_length();
}

Field_long::Field_long(const std::string& field_name_arg, const std::string& type):
    Field_num(field_name_arg, type) {}

const char* Field_long::unpack(const char* from) {

    uint32 tmp = uint4korr(from);
    field_data = tmp;

    LOG_TRACE(log, "  long: " << tmp << " // " << pack_length());

    return from + pack_length();
}

Field_longlong::Field_longlong(const std::string& field_name_arg, const std::string& type):
    Field_num(field_name_arg, type) {}

const char* Field_longlong::unpack(const char* from) {

    ulonglong tmp = uint8korr(from);
    field_data = tmp;

    LOG_TRACE(log, "  longlong: " << tmp << " // " << pack_length());

    return from + pack_length();
}

Field_real::Field_real(const std::string& field_name_arg, const std::string& type):
    Field_num(field_name_arg, type) {}

Field_double::Field_double(const std::string& field_name_arg, const std::string& type):
    Field_real(field_name_arg, type) {}

const char* Field_double::unpack(const char* from) {

    double tmp = *((double*)(from));
    field_data = tmp;

    LOG_TRACE(log, "  double: " << tmp << " // " << pack_length());

    return from + pack_length();
}


Field_float::Field_float(const std::string& field_name_arg, const std::string& type):
    Field_real(field_name_arg, type) {}

const char* Field_float::unpack(const char* from) {

    float tmp = *((float*)(from));
    field_data = tmp;

    LOG_TRACE(log, "  float: " << tmp << " // " << pack_length());

    return from + pack_length();
}


Field_str::Field_str(const std::string& field_name_arg, const std::string& type):
    Field(field_name_arg, type) {}

Field_timestamp::Field_timestamp(const std::string& field_name_arg, const std::string& type):
    Field_str(field_name_arg, type) {}

const char* Field_timestamp::unpack(const char* from) {

    uint32 tmp = uint4korr(from);
    field_data = tmp;

    LOG_TRACE(log, "  timestamp: " << tmp << " // " << pack_length());

    return from + pack_length();
}

Field_year::Field_year(const std::string& field_name_arg, const std::string& type):
    Field_tiny(field_name_arg, type) {}

Field_datetime::Field_datetime(const std::string& field_name_arg, const std::string& type):
    Field_str(field_name_arg, type) {}

const char* Field_datetime::unpack(const char* from) {

    ulonglong tmp = uint8korr(from);
    field_data = tmp;

    LOG_TRACE(log, "  datetime: " << tmp << " // " << pack_length());

    return from + pack_length();
}

Field_date::Field_date(const std::string& field_name_arg, const std::string& type):
    Field_str(field_name_arg, type) {}

const char* Field_date::unpack(const char* from) {

    uint32 tmp = uint3korr(from);
    field_data = tmp;

    LOG_TRACE(log, "  date: " << tmp << " // " << pack_length());

    return from + pack_length();
}

Field_time::Field_time(const std::string& field_name_arg, const std::string& type):
    Field_str(field_name_arg, type) {}

const char* Field_time::unpack(const char* from) {

    uint32 tmp = uint3korr(from);
    field_data = tmp;

    LOG_TRACE(log, "  time: " << tmp << " // " << pack_length());

    return from + pack_length();
}

Field_enum::Field_enum(const std::string& field_name_arg, const std::string& type):
    Field_str(field_name_arg, type) {

    //для поля типа enum нужно подсчитать количество перечисляемых значений
    // если количество перечислений меньше 255, то этот тип занимает 1 байт
    count_elements = 1;
    for (std::string::const_iterator i = type.begin(); i != type.end(); ++i) {
        if (*i == ',') {
            count_elements++;
        }
    }
}

const char* Field_enum::unpack(const char* from) {

    int tmp;

    if (pack_length() == 1) {

        //для удобства использования в mysql++ приводим к int
        tmp = int(*((char*)(from)));

    } else {
        tmp = int(*((short*)(from)));
    }

    field_data = tmp;

    LOG_TRACE(log, "  enum: " << tmp << " // " << pack_length());

    return from + pack_length();
}

Field_set::Field_set(const std::string& field_name_arg, const std::string& type):
    Field_enum(field_name_arg, type) {

    //для поля типа set нужно подсчитать количество элементов множества
    //определяется по формуле (N+7)/8

    count_elements = 1;
    for (std::string::const_iterator i = type.begin(); i != type.end(); ++i) {
        if (*i == ',') {
            count_elements++;
        }
    }
}

const char* Field_set::unpack(const char* from) {

    //для удобства использования в mysql++ приводим к типу unsigned long long
    ulonglong tmp;

    switch(pack_length()) {
    case 1:
        tmp = ulonglong(*((char*)(from)));
        break;
    case 2:
        tmp = ulonglong(uint2korr(from));
        break;
    case 3:
        tmp = ulonglong(uint3korr(from));
        break;
    case 4:
        tmp = ulonglong(uint4korr(from));
        break;
    case 8:
        tmp = uint8korr(from);
        break;
    default:
        tmp = uint8korr(from);
        break;
    }

    field_data = tmp;

    LOG_TRACE(log, "  set: " << tmp << " // " << pack_length());

    return from + pack_length();
}

Field_longstr::Field_longstr(const std::string& field_name_arg, const std::string& type):
    Field_str(field_name_arg, type)  {}

Field_varstring::Field_varstring(const std::string& field_name_arg, const std::string& type, const collate_info& collate):
    Field_longstr(field_name_arg, type) {

    //для этого поля количество байт определяется исходя из количества элементов в строке

    std::string::size_type b = type.find('(', 0);
    std::string::size_type e = type.find(')', 0);

    if (b == std::string::npos || e == std::string::npos) {
        throw std::runtime_error("Field_string: Incorrect field VARCHAR");
    }

    //получаем размер строки
    std::string str_count(type, b+1, e-b-1);
    int symbols = atoi(str_count.c_str());
    int bytes = symbols * collate.maxlen;

    //количество байт, занимаемых в памяти

    length_bytes = ((bytes < 256) ? 1 : 2);

    //длина символов
    field_length = symbols;
}

const char* Field_varstring::unpack(const char* from) {

    unsigned length_row;
    if (length_bytes == 1) {
        //length_row = (unsigned int) (unsigned char) (*to = *from++);
        length_row = (unsigned int) (unsigned char) (*from++);

    } else {
        length_row = uint2korr(from);
        from++;
        from++;
    }

    std::string tmp(from, length_row);
    field_data = tmp;

    LOG_TRACE(log, "  varstr: '" << tmp << "' // " << length_bytes << " " << length_row);

    return from + length_row;
}


Field_blob::Field_blob(const std::string& field_name_arg, const std::string& type):
    Field_longstr(field_name_arg, type), packlength(2) {}

Field_tinyblob::Field_tinyblob(const std::string& field_name_arg, const std::string& type):
    Field_blob(field_name_arg, type) { packlength = 1; }

Field_mediumblob::Field_mediumblob(const std::string& field_name_arg, const std::string& type):
    Field_blob(field_name_arg, type) { packlength = 3; }

Field_longblob::Field_longblob(const std::string& field_name_arg, const std::string& type):
    Field_blob(field_name_arg, type) { packlength = 4; }

const char* Field_blob::unpack(const char* from) {

    const unsigned length_row = get_length(from);
    from += packlength;

    std::string tmp(from, length_row);
    field_data = tmp;

    LOG_TRACE(log, "  blob: '" << tmp << "' // " << packlength << " " << length_row);

    return from + length_row;
}


unsigned int Field_blob::get_length(const char *pos) {

    switch (packlength)
    {
    case 1:
        return (unsigned int) (unsigned char) pos[0];
    case 2:
    {

        unsigned short tmp = 0;

        /*
        if (db_low_byte_first) {
            tmp = sint2korr(pos);
        } else {
            shortget(tmp,pos);
        }
        */
        tmp = sint2korr(pos);

        return (unsigned int) tmp;

    }
    case 3:
        return (unsigned int) uint3korr(pos);
    case 4:
    {
        unsigned int tmp;

        /*
        if (db_low_byte_first)
            tmp = uint4korr(pos);
        else
            longget(tmp,pos);
        */
        tmp = uint4korr(pos);

        return (unsigned int) tmp;
    }

    }

    throw std::runtime_error("Oops, wrong packlength in Field_blob::get_length(): wanted 1, 2, 3 or 4.");
}

Field_decimal::Field_decimal(const std::string& field_name_arg, const std::string& type):
    Field_longstr(field_name_arg, type)
{
    // Получаем размеры поля: decimal(M,D)
    // M - общее количество цифр, M-D - после запятой

    const std::string::size_type b = type.find('(', 0);

    if (b == std::string::npos) {
        throw std::runtime_error("Field_string: Incorrect field DECIMAL");
    }

    int m, d;
    if (2 != sscanf(type.c_str() + b, "(%d,%d)", &m, &d) || m <= 0 || m < d) {
        throw std::runtime_error("Field_string: Incorrect field DECIMAL");
    }

    const int intg = m - d;
    const int frac = d;

    static const int dig2bytes[] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
    field_length = (intg / 9) * 4 + dig2bytes[intg % 9] + (frac / 9) * 4 + dig2bytes[frac % 9];
}

const char* Field_decimal::unpack(const char *from)
{
    return from + pack_length();
}

}

