
#include <unistd.h>
#include <iostream>
#include <sstream>

#include "Slave.h"


std::string print(const std::string& type, const boost::any& v) {
    
    std::cout << type << " " << v.type().name() << std::endl;

    if (v.type() == typeid(std::string)) {

        std::string r = "'";
        r += boost::any_cast<std::string>(v);
        r += "'";
        return r;

    } else {
        std::ostringstream s;

        if (v.type() == typeid(int)) 
            s << boost::any_cast<int>(v);

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

        std::cout << "  " << i->first << " : " << i->second.first << " -> " << print(i->second.first, i->second.second);

        if (event.type_event == slave::RecordSet::Update) {

            slave::Row::const_iterator j = event.m_old_row.find(i->first);

            std::cout << "    (was: " << (j == event.m_old_row.end() ? "NULL" : print(i->second.first, j->second.second)) << ")";
        }

        std::cout << "\n";
    }

    std::cout << "  @ts = "  << event.when << "\n"
         << "  @server_id = " << event.master_id << "\n\n";
}

void xid_callback(unsigned int server_id) {

    std::cout << "COMMIT @server_id = " << server_id << "\n\n";
}


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

    masterinfo.master_info_file = "libslave.master_info";

    try {

        std::cout << "Creating client, setting callbacks..." << std::endl;
    
        slave::Slave slave(masterinfo);

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
