#define BOOST_TEST_MODULE libslave
#define BOOST_TEST_DYN_LINK
#include <boost/test/unit_test.hpp>

using namespace boost;
using namespace boost::unit_test;

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/mpl/int.hpp>
#include <boost/mpl/list.hpp>
#include <boost/thread.hpp>
#include "Slave.h"
#include "nanomysql.h"

namespace
{
    const std::string TestDataDir = "test/data/";

    struct config
    {
        std::string mysql_host;
        int         mysql_port;
        std::string mysql_db;
        std::string mysql_user;
        std::string mysql_pass;

        config()
        :   mysql_host("localhost")
        ,   mysql_port(3306)
        ,   mysql_db("test")
        ,   mysql_user("root")
        {}

        void load(const std::string& fn)
        {
            std::ifstream f(fn.c_str());
            if (!f)
                throw std::runtime_error("can't open config file '" + fn + "'");

            std::string line;
            while (getline(f, line))
            {
                if (line.empty())
                    continue;
                std::vector<std::string> tokens;
                boost::algorithm::split(tokens, line, boost::algorithm::is_any_of(" ="), boost::algorithm::token_compress_on);
                if (tokens.empty())
                    continue;
                if (tokens.size() != 2)
                    throw std::runtime_error("Malformed string '" + line + "' in the config file '" + fn + "'");

                if (tokens.front() == "mysql_host")
                    mysql_host = tokens.back();
                else if (tokens.front() == "mysql_port")
                    mysql_port = atoi(tokens.back().c_str());
                else if (tokens.front() == "mysql_db")
                    mysql_db = tokens.back();
                else if (tokens.front() == "mysql_user")
                    mysql_user = tokens.back();
                else if (tokens.front() == "mysql_pass")
                    mysql_pass = tokens.back();
                else
                    throw std::runtime_error("unknown option '" + tokens.front() + "' in config file '" + fn + "'");
            }
        }
    };

    template <typename T>
    class Atomic
    {
        volatile T m_Value;

    public:
        typedef T value_type;

        Atomic() {}
        Atomic(T aValue) : m_Value(aValue) {}
        Atomic(const Atomic& r) : m_Value(r) {} // использует operator T
        Atomic& operator= (const Atomic& r) { return operator = (static_cast<T>(r)); }

        T operator++ ()         { return __sync_add_and_fetch(&m_Value, 1); }
        T operator++ (int)      { return __sync_fetch_and_add(&m_Value, 1); }
        T operator+= (T aValue) { return __sync_add_and_fetch(&m_Value, aValue); }

        T operator-- ()         { return __sync_sub_and_fetch(&m_Value, 1); }
        T operator-- (int)      { return __sync_fetch_and_sub(&m_Value, 1); }
        T operator-= (T aValue) { return __sync_sub_and_fetch(&m_Value, aValue); }

        Atomic& operator= (T aValue) { __sync_lock_test_and_set(&m_Value, aValue); return *this; }

        operator T() const
        {
            __sync_synchronize();
            return m_Value;
        }
    };

    struct Fixture
    {
        config cfg;
        slave::Slave m_Slave;
        boost::shared_ptr<nanomysql::Connection> conn;

        struct StopFlag
        {
            Atomic<int> m_StopFlag;
            Atomic<int> m_SlaveStarted;

            StopFlag() : m_StopFlag(false), m_SlaveStarted(false) {}

            bool operator() ()
            {
                m_SlaveStarted = true;
                return m_StopFlag;
            }
        };

        StopFlag        m_StopFlag;
        boost::thread   m_SlaveThread;

        struct Callback
        {
            boost::mutex m_Mutex;
            slave::callback m_Callback;
            Atomic<int> m_UnwantedCalls;

            Callback() : m_UnwantedCalls(0) {}

            void operator() (slave::RecordSet& rs)
            {
                boost::mutex::scoped_lock l(m_Mutex);
                if (!m_Callback.empty())
                    m_Callback(rs);
                else
                    ++m_UnwantedCalls;
            }

            void setCallback(slave::callback c)
            {
                boost::mutex::scoped_lock l(m_Mutex);
                m_Callback = c;
            }

            void setCallback()
            {
                boost::mutex::scoped_lock l(m_Mutex);
                m_Callback.clear();
            }
        };

        Callback m_Callback;

        Fixture()
        {
            cfg.load(TestDataDir + "mysql.conf");

            conn.reset(new nanomysql::Connection(cfg.mysql_host, cfg.mysql_user, cfg.mysql_pass, cfg.mysql_db));
            conn->query("set names utf8");
            // Создаем таблицу, т.к. если ее нет, libslave ругнется на ее отсутствие, и тест закончится
            conn->query("CREATE TABLE IF NOT EXISTS test (tmp int)");

            slave::MasterInfo sMasterInfo;
            sMasterInfo.host = cfg.mysql_host;
            sMasterInfo.port = cfg.mysql_port;
            sMasterInfo.user = cfg.mysql_user;
            sMasterInfo.password = cfg.mysql_pass;

            m_Slave = slave::Slave(sMasterInfo);
            // Ставим колбек из фиксчи - а он будет вызывать колбеки, которые ему будут ставить в тестах
            m_Slave.setCallback(cfg.mysql_db, "test", boost::ref(m_Callback));
            m_Slave.init();
            m_Slave.createDatabaseStructure();

            // Запускаем libslave с нашим кастомной функцией остановки, которая еще и сигнализирует,
            // когда слейв прочитал позицию бинлога и готов получать сообщения
            // NOTE два boost::ref необходимы, т.к. один для boost:bind, другой для boost::function
            m_SlaveThread = boost::thread(boost::bind(&slave::Slave::get_remote_binlog, boost::ref(m_Slave), boost::ref(boost::ref(m_StopFlag))));

            // Ждем, чтобы libslave запустился - не более 1000 раз по 1 мс
            const timespec ts = {0 , 1000000};
            size_t i = 0;
            for (; i < 1000; ++i)
            {
                ::nanosleep(&ts, NULL);
                if (m_StopFlag.m_SlaveStarted)
                    break;
            }
            if (1000 == i)
                BOOST_FAIL ("Can't connect to mysql via libslave in 1 second");
        }

        ~Fixture()
        {
            m_StopFlag.m_StopFlag = true;
            m_Slave.close_connection();
            m_SlaveThread.join();
        }
    };

    BOOST_FIXTURE_TEST_SUITE(Slave, Fixture)

    BOOST_AUTO_TEST_CASE(test_HelloWorld)
    {
        std::cout << "You probably should specify parameters to mysql in the file " << TestDataDir << "mysql.conf first" << std::endl;
    }

    enum MYSQL_TYPE
    {
        MYSQL_TINYINT,
        MYSQL_INT,
        MYSQL_BIGINT,
        MYSQL_CHAR,
        MYSQL_VARCHAR,
        MYSQL_TINYTEXT,
        MYSQL_TEXT
    };

    template <MYSQL_TYPE T>
    struct MYSQL_type_traits;

    template <>
    struct MYSQL_type_traits<MYSQL_INT>
    {
        typedef uint32_t slave_type;
        static const std::string name;
    };
    const std::string MYSQL_type_traits<MYSQL_INT>::name = "INT";

    template <>
    struct MYSQL_type_traits<MYSQL_BIGINT>
    {
        typedef unsigned long long slave_type;
        static const std::string name;
    };
    const std::string MYSQL_type_traits<MYSQL_BIGINT>::name = "BIGINT";

    template <>
    struct MYSQL_type_traits<MYSQL_CHAR>
    {
        typedef std::string slave_type;
        static const std::string name;
    };
    const std::string MYSQL_type_traits<MYSQL_CHAR>::name = "CHAR";

    template <>
    struct MYSQL_type_traits<MYSQL_VARCHAR>
    {
        typedef std::string slave_type;
        static const std::string name;
    };
    const std::string MYSQL_type_traits<MYSQL_VARCHAR>::name = "VARCHAR";

    template <>
    struct MYSQL_type_traits<MYSQL_TINYTEXT>
    {
        typedef std::string slave_type;
        static const std::string name;
    };
    const std::string MYSQL_type_traits<MYSQL_TINYTEXT>::name = "TINYTEXT";

    template <>
    struct MYSQL_type_traits<MYSQL_TEXT>
    {
        typedef std::string slave_type;
        static const std::string name;
    };
    const std::string MYSQL_type_traits<MYSQL_TEXT>::name = "TEXT";

    template <typename T>
    struct CheckEquality
    {
        T value;
        Atomic<int> counter;
        std::string fail_reason;

        CheckEquality(const T& t) : value(t), counter(0) {}

        void operator() (const slave::RecordSet& rs)
        {
            try
            {
                if (++counter > 1)
                    throw std::runtime_error("Second call on CheckEquality");
                const slave::Row& row = rs.m_row;
                if (row.size() > 1)
                {
                    std::ostringstream str;
                    str << "Row size is " << row.size();
                    throw std::runtime_error(str.str());
                }
                const slave::Row::const_iterator it = row.find("value");
                if (row.end() == it)
                    throw std::runtime_error("Can't find field 'value' in the row");
                const T t = boost::any_cast<T>(it->second.second);
                if (value != t)
                {
                    std::ostringstream str;
                    str << "Value '" << value << "' is not equal to libslave value '" << t << "'";
                    throw std::runtime_error(str.str());
                }
            }
            catch (const std::exception& ex)
            {
                fail_reason += '\n';
                fail_reason += ex.what();
            }
        }
    };

    template <typename T>
    void getValue(const std::string& s, T& t)
    {
        std::istringstream is;
        is.str(s);
        is >> t;
    }

    void getValue(const std::string& s, std::string& t)
    {
        t = s;
        // Убираем ведущий пробел
        t.erase(0, 1);
    }

    typedef boost::mpl::list<
        boost::mpl::int_<MYSQL_INT>,
        boost::mpl::int_<MYSQL_BIGINT>,
        boost::mpl::int_<MYSQL_CHAR>,
        boost::mpl::int_<MYSQL_VARCHAR>,
        boost::mpl::int_<MYSQL_TINYTEXT>,
        boost::mpl::int_<MYSQL_TEXT>
    > mysql_one_field_types;

    BOOST_AUTO_TEST_CASE_TEMPLATE(test_OneField, T, mysql_one_field_types)
    {
        typedef MYSQL_type_traits<MYSQL_TYPE(T::value)> type_traits;
        typedef typename type_traits::slave_type slave_type;

        const std::string sDataFilename = TestDataDir + "OneField/" + type_traits::name;
        std::ifstream f(sDataFilename.c_str());
        BOOST_REQUIRE_MESSAGE(f, "Cannot open file with data: '" << sDataFilename << "'");
        std::string line;
        size_t line_num = 0;
        while (getline(f, line))
        {
            ++line_num;
            if (line.empty())
                continue;
            std::vector<std::string> tokens;
            boost::algorithm::split(tokens, line, boost::algorithm::is_any_of(","), boost::algorithm::token_compress_on);
            if (tokens.empty())
                continue;
            if (tokens.front() == "define")
            {
                if (tokens.size() != 2)
                    BOOST_FAIL("Malformed string '" << line << "' in the file '" << sDataFilename << "'");
                const std::string sDropTableQuery = "DROP TABLE IF EXISTS test";
                conn->query(sDropTableQuery);
                const std::string sCreateTableQuery = "CREATE TABLE test (value " + tokens[1] + ") DEFAULT CHARSET=utf8";
                conn->query(sCreateTableQuery);
            }
            else if (tokens.front() == "data")
            {
                if (tokens.size() != 3)
                    BOOST_FAIL("Malformed string '" << line << "' in the file '" << sDataFilename << "'");

                // Получаем значение, с которым надо будет сравнить значение из libslave
                slave_type checked_value;
                getValue(tokens[2], checked_value);

                // Устанавливаем в libslave колбек для проверки этого значения
                CheckEquality<slave_type> sCallback(checked_value);
                m_Callback.setCallback(boost::ref(sCallback));
                // Проверяем, что не было нежелательных вызовов до этого
                if (0 != m_Callback.m_UnwantedCalls)
                {
                    BOOST_ERROR("Unwanted calls before this case: " << m_Callback.m_UnwantedCalls
                            << " (we are now on file '" << sDataFilename << "' line " << line_num << ": '" << line << "'");
                }

                // Всталяем значение в таблицу
                const std::string sInsertValueQuery = "INSERT INTO test VALUES (" + tokens[1] + ")";
                conn->query(sInsertValueQuery);

                // Ждем отработки колбека максимум 1 секунду
                const timespec ts = {0 , 1000000};
                size_t i = 0;
                for (; i < 1000; ++i)
                {
                    ::nanosleep(&ts, NULL);
                    if (sCallback.counter >= 1)
                        break;
                }
                if (sCallback.counter < 1)
                    BOOST_ERROR ("Have no calls to libslave callback for 1 second");

                // Убираем наш колбек, т.к. он при выходе из блока уничтожится, заодно чтобы
                // строку он не мучал больше, пока мы ее проверяем
                m_Callback.setCallback();

                if (!sCallback.fail_reason.empty())
                    BOOST_ERROR(sCallback.fail_reason
                            << "\n(we are now on file '" << sDataFilename << "' line " << line_num << ": '" << line << "'");
            }
            else if (tokens.front()[0] == ';')
                continue;   // комментарий
            else
                BOOST_FAIL("Unknown command '" << tokens.front() << "' in the file '" << sDataFilename << "' on line " << line_num);
        }
    }

    BOOST_AUTO_TEST_SUITE_END()
}// anonymous-namespace
