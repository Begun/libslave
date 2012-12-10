
#include <unistd.h>
#include <iostream>
#include <sstream>

#include "Slave.h"


std::string print(const std::string& type, const boost::any& v) {
    
    if (v.type() == typeid(std::string)) {

        std::string r = "'";
        r += boost::any_cast<std::string>(v);
        r += "'";
        return r;

    } else {
        std::ostringstream s;

        if (v.type() == typeid(int)) 
            s << boost::any_cast<int>(v);

        else if (v.type() == typeid(unsigned int))
            s << boost::any_cast<unsigned int>(v);

        else if (v.type() == typeid(double))
            s << boost::any_cast<double>(v);

        else if (v.type() == typeid(unsigned long long))
            s << boost::any_cast<unsigned long long>(v);

        else
            s << boost::any_cast<long>(v);

        return s.str();
    }
}


void callback(const slave::RecordSet& event) {

    switch (event.type_event) {
    case slave::RecordSet::Update: std::cout << "UPDATE"; break;
    case slave::RecordSet::Delete: std::cout << "DELETE"; break;
    case slave::RecordSet::Write:  std::cout << "INSERT"; break;
    default: break;
    }

    std::cout << " " << event.db_name << "." << event.tbl_name << "\n"; 

    for (slave::Row::const_iterator i = event.m_row.begin(); i != event.m_row.end(); ++i) {

        std::string value = print(i->second.first, i->second.second);

        std::cout << "  " << i->first << " : " << i->second.first << " -> " << value;

        if (event.type_event == slave::RecordSet::Update) {

            slave::Row::const_iterator j = event.m_old_row.find(i->first);

            std::string old_value("NULL");

            if (j != event.m_old_row.end()) 
                old_value = print(i->second.first, j->second.second);

            if (value != old_value) 
                std::cout << "    (was: " << old_value << ")";
        }

        std::cout << "\n";
    }

    std::cout << "  @ts = "  << event.when << "\n"
         << "  @server_id = " << event.master_id << "\n\n";
}

void xid_callback(unsigned int server_id) {

    std::cout << "COMMIT @server_id = " << server_id << "\n\n";
}


struct ExampleState : public slave::ExtStateIface {

    slave::State state;

    virtual slave::State getState() { return state; }

    // Counts and timestamps of connects to mysql master.
    virtual void setConnecting() { 
        state.connect_time = ::time(NULL);
        state.connect_count++;
    }

    virtual time_t getConnectTime() { return state.connect_time; }
    virtual unsigned int getConnectCount() { return state.connect_count; }

    // Timestamp of last actually handled event.
    virtual void setLastFilteredUpdateTime() { state.last_filtered_update = ::time(NULL); }
    virtual time_t getLastFilteredUpdateTime() { return state.last_filtered_update; }

    // Position and local and remote time of last event read in binlog.
    virtual void setLastEventTimePos(time_t t, unsigned long pos) {
        state.last_event_time = t;
        state.last_update = ::time(NULL);
        state.intransaction_pos = pos;
    }

    virtual time_t getLastUpdateTime() { return state.last_update; }
    virtual time_t getLastEventTime() { return state.last_event_time; }
    virtual unsigned long getIntransactionPos() { return state.intransaction_pos; }

    // Set and get binlog position in memory. (For rollback.)
    virtual void setMasterLogNamePos(const std::string& log_name, unsigned long pos) {
        state.master_log_name = log_name;
        state.master_log_pos = pos;
    }

    virtual unsigned long getMasterLogPos() { return state.master_log_pos; }
    virtual std::string getMasterLogName() { return state.master_log_name; }

    // Persist the binlog position on disk. (For rollback.)
    virtual void saveMasterInfo() {}
    virtual bool loadMasterInfo(std::string& logname, unsigned long& pos) { return false; }

    // False if we are currently waiting for binlog data from the network.
    virtual void setStateProcessing(bool _state) { state.state_processing = _state; }
    virtual bool getStateProcessing() { return state.state_processing; }

    // For counting table names in the binlog stream.
    virtual void initTableCount(const std::string& t) {}
    virtual void incTableCount(const std::string& t) {}
};


int main(int argc, char** argv) {

    std::string host;
    std::string user;
    std::string password;
    std::string database;
    unsigned int port = 3306;

    while (1) {
        int c = ::getopt(argc, argv, "h:u:p:P:d:");

        if (c == -1) 
            break;

        switch (c) {
        case 'h':
            host = optarg;
            break;

        case 'u':
            user = optarg;
            break;

        case 'p':
            password = optarg;
            break;

        case 'd':
            database = optarg;

        case 'P':
            port = ::atoi(optarg);
            break;

        default:
            std::cout << "Usage: libslave_test -h <mysql host> -u <mysql user> -p <mysql password> -d <mysql database> "
                      << "<table name> <table name> ..." << std::endl;
            return 1;
        }
    }

    if (host.empty() || user.empty() || database.empty() || password.empty()) {
        std::cout << "Usage: libslave_test -h <mysql host> -u <mysql user> -p <mysql password> -d <mysql database> "
                  << "<table name> <table name> ..." << std::endl;
        return 1;
    }

    std::vector<std::string> tables;

    while (optind < argc) {
        tables.push_back(argv[optind]);
        optind++;
    }

    /////  Real work starts here.


    slave::MasterInfo masterinfo;

    masterinfo.host = host;
    masterinfo.port = port;
    masterinfo.user = user;
    masterinfo.password = password;

    ExampleState state_and_stats;

    try {

        std::cout << "Creating client, setting callbacks..." << std::endl;
    
        slave::Slave slave(masterinfo, state_and_stats);

        for (std::vector<std::string>::const_iterator i = tables.begin(); i != tables.end(); ++i) {
            slave.setCallback(database, *i, callback);
        }

        slave.setXidCallback(xid_callback);

        std::cout << "Initializing client..." << std::endl;
        slave.init();

        std::cout << "Reading database structure..." << std::endl;
        slave.createDatabaseStructure();

        while (1) {
            try {
                
                std::cout << "Reading binlogs..." << std::endl;
                slave.get_remote_binlog();

            } catch (std::exception& ex) {
                std::cout << "Error in reading binlogs: " << ex.what() << std::endl;
            }
        }

    } catch (std::exception& ex) {
        std::cout << "Error in initializing slave: " << ex.what() << std::endl;
    }
    
    return 0;
}
