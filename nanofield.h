#ifndef __SLAVE_NANO_FIELD_H__
#define __SLAVE_NANO_FIELD_H__

#include <map>
#include <string>
#include <sstream>

namespace nanomysql
{
    struct field
    {
        std::string name;
        size_t type;
        std::string data;

        field(const std::string& n, size_t t) : name(n), type(t) {}

        template <typename T>
        operator T() const
        {
            T ret;
            std::istringstream s;
            s.str(data);
            s >> ret;
            return ret;
        }
    };

    typedef std::map<std::string, field> fields_t;
}// nanomysql

#endif
