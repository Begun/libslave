#include <map>
#include <mysql/mysql.h>
#include <stdexcept>
#include <stdio.h>
#include <string>
#include <vector>
#include "nanomysql.h"
#include "collate.h"

using namespace slave;

collate_map_t slave::readCollateMap(nanomysql::Connection& conn)
{
    collate_map_t res;
    nanomysql::Connection::result_t nanores;

    typedef std::map<std::string, int> charset_maxlen_t;
    charset_maxlen_t cm;

    conn.query("SHOW CHARACTER SET");
    conn.store(nanores);

    for (nanomysql::Connection::result_t::const_iterator i = nanores.begin(); i != nanores.end(); ++i)
    {
        std::map<std::string, nanomysql::field>::const_iterator z = i->find("Charset");
        if (z == i->end())
            throw std::runtime_error("Slave::readCollateMap(): SHOW CHARACTER SET query did not return 'Charset'");
        const std::string name = z->second.data;

        z = i->find("Maxlen");
        if (z == i->end())
            throw std::runtime_error("Slave::readCollateMap(): SHOW CHARACTER SET query did not return 'Maxlen'");

        const int maxlen = atoi(z->second.data.c_str());

        cm[name] = maxlen;
    }

    nanores.clear();
    conn.query("SHOW COLLATION");
    conn.store(nanores);

    for (nanomysql::Connection::result_t::const_iterator i = nanores.begin(); i != nanores.end(); ++i)
    {
        collate_info ci;

        std::map<std::string, nanomysql::field>::const_iterator z = i->find("Collation");
        if (z == i->end())
            throw std::runtime_error("Slave::readCollateMap(): SHOW COLLATION query did not return 'Collation'");
        ci.name = z->second.data;

        z = i->find("Charset");
        if (z == i->end())
            throw std::runtime_error("Slave::readCollateMap(): SHOW COLLATION query did not return 'Charset'");
        ci.charset = z->second.data;

        charset_maxlen_t::const_iterator j = cm.find(ci.charset);
        if (j == cm.end())
            throw std::runtime_error("Slave::readCollateMap(): SHOW COLLATION returns charset not shown in SHOW CHARACTER SET"
                    " (collation '" + ci.name + "', charset '" + ci.charset + "')");

        ci.maxlen = j->second;

        res[ci.name] = ci;
    }

    return res;
}
