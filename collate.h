#ifndef __SLAVE_COLLATE_H
#define __SLAVE_COLLATE_H

#include <map>
#include <string>

namespace nanomysql
{
    struct Connection;
}

namespace slave
{
    struct collate_info
    {
        std::string name;
        std::string charset;
        int maxlen;

        collate_info() : maxlen(0) {}
    };

    typedef std::map<std::string, collate_info> collate_map_t;

    collate_map_t readCollateMap(nanomysql::Connection& conn);
}// slave

#endif
