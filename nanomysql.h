#ifndef __NANOMYSQL_H
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

#define __NANOMYSQL_H

#include <boost/bind.hpp>
#include <mysql/mysql.h>
#include "nanofield.h"
#include <stdexcept>
#include <stdio.h>
#include <vector>

namespace nanomysql {

struct Connection {

private:
    void throw_error(std::string msg, const std::string& m2 = "") {

        msg += ": ";
        msg += ::mysql_error(m_conn);
        msg += " : ";

        char n[32];
        ::snprintf(n, 31, "%d", ::mysql_errno(m_conn));
        msg += n;

        if (m2.size() > 0) {
            msg += " : [";
            msg += m2;
            msg += "]";
        }

        throw std::runtime_error(msg);
    }

    struct _mysql_res_wrap {
        MYSQL_RES* s;
        _mysql_res_wrap(MYSQL_RES* _s) : s(_s) {}
        ~_mysql_res_wrap() { if (s != NULL) ::mysql_free_result(s); }
    };

public:
    Connection(const std::string& host, const std::string& user, const std::string& password,
               const std::string& db = "", int port = 0) {

        m_conn = ::mysql_init(NULL);

        if (!m_conn)
            throw std::runtime_error("Could not mysql_init()");

        if (::mysql_real_connect(m_conn, host.c_str(), user.c_str(), password.c_str(), db.c_str(), port, NULL, 0) == NULL) {
            throw_error("Could not mysql_real_connect()");
        }
    }

    ~Connection() {
        ::mysql_close(m_conn);
    }

    void query(const std::string& q) {

        if (::mysql_real_query(m_conn, q.data(), q.size()) != 0) {
            throw_error("mysql_query() failed", q);
        }
    }

    template <typename F>
    void use(F f) {

        _mysql_res_wrap re(::mysql_use_result(m_conn));

        if (re.s == NULL) {
            throw_error("mysql_use_result() failed");
        }

        const size_t num_fields = ::mysql_num_fields(re.s);

        fields_t fields;
        std::vector<std::map<std::string,field>::iterator> fields_n;

        while (1) {
            MYSQL_FIELD* ff = ::mysql_fetch_field(re.s);

            if (!ff) break;

            fields_n.push_back(
                fields.insert(fields.end(),
                              std::make_pair(ff->name,
                                             field(ff->name, ff->type))));
        }

        while (1) {
            MYSQL_ROW row = ::mysql_fetch_row(re.s);

            if (row == NULL) {
                if (::mysql_errno(m_conn) != 0) {
                    throw_error("mysql_fetch_row() failed");
                }

                break;
            }

            const unsigned long* lens = ::mysql_fetch_lengths(re.s);

            for (size_t z = 0; z != num_fields; ++z) {
                fields_n[z]->second.data.assign(row[z], lens[z]);
            }

            f(fields);
        }
    }

    typedef std::vector<std::map<std::string, field> > result_t;

    void store(result_t& out) {
#ifndef __GXX_EXPERIMENTAL_CXX0X__
        use(boost::bind(&result_t::push_back, &out, _1));
#else
        use( [&out] (const std::map<std::string,field>& f) { out.push_back(f); } );
#endif

    }


    MYSQL* m_conn;
};

}

#endif
