
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

#ifndef __SLAVE_SLAVESTATS_H_
#define __SLAVE_SLAVESTATS_H_

#include <map>
#include <iostream>
#include <fstream>


/*
 * WARNING WARNING WARNING
 *
 * This file is intended as a demonstration only.
 * The code here works, but is not thread-safe or production-ready.
 * Please provide your own implementation to fill the gaps according
 * to your particular project's needs.
 */


namespace slave {

namespace stats {

class SlaveStats {
public:

    std::string connection_host;
    std::string master_log_name;
    unsigned long master_log_pos;
    unsigned int reconnect_count;
    bool state_processing;
    time_t last_event_time;
    time_t last_update;
    time_t last_filtered_update;
    time_t time_connect;

    std::map<std::string,size_t> table_counts;

    std::string master_info_file;

    SlaveStats() :
        master_log_pos(0),
        reconnect_count(0),
        state_processing(false),
        last_event_time(0),
        last_update(0),
        last_filtered_update(0),
        time_connect(0)
        {}

    void writeMasterInfoFile() {

        std::ofstream f(master_info_file.c_str(), std::ios::out | std::ios::trunc);

        if (!f)
            throw std::runtime_error("slave::writeMasterInfoFile(): Could not open file: " + master_info_file);

        f << master_log_pos << '\n' << master_log_name << '\n';
    }


    void readMasterInfoFile(std::string& logname, unsigned long& pos) {

        std::ifstream f(master_info_file.c_str());

        if (!f) {
            pos = 0;
            return;
        }

        f >> pos;

        size_t n = f.tellg();
        f.seekg(0, std::ios::end);
        size_t e = f.tellg();
        f.seekg(n+1, std::ios::beg);

        logname.resize(e - n - 1);

        f.getline((char*)logname.c_str(), logname.size());

        master_log_pos = pos;
        master_log_name = logname;
    }

};

///

inline SlaveStats& stats() {
    static SlaveStats _stats;
    return _stats;
}

inline void setConnectTime() { stats().time_connect = ::time(NULL); }

inline void setHost(const std::string& h) { stats().connection_host = h; }

inline void setLastFilteredUpdateTime() { stats().last_filtered_update = ::time(NULL); }

inline void setMasterInfoFile(const std::string& f) { stats().master_info_file = f; }

inline void setMasterLogName(const std::string& f) { stats().master_log_name = f; }

inline void setMasterLogPos(unsigned int p) { stats().master_log_pos = p; }

inline void setReconnectCount() { stats().reconnect_count++; }

inline void setLastEventTime(time_t d) {
    stats().last_event_time = d;
    stats().last_update = ::time(NULL);
}

inline void setStateProcessing(bool p) { stats().state_processing = p; }

inline void writeMasterInfoFile() { stats().writeMasterInfoFile(); }

inline void readMasterInfoFile(std::string& f, unsigned long& pos) { stats().readMasterInfoFile(f, pos); }

inline void initTableCount(const std::string& t) {  stats().table_counts[t] = 0; }

inline void incTableCount(const std::string& t) { stats().table_counts[t]++; }

}

}


#endif
