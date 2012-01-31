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


/*
 * WARNING WARNING WARNING
 *
 * This file is intended as a demonstration only.
 * The code here works, but is not thread-safe or production-ready.
 * Please provide your own implementation to fill the gaps according
 * to your particular project's needs.
 */


#include <sys/time.h>


namespace slave
{

struct MasterInfo {

    std::string host;
    unsigned int port;
    std::string user;
    std::string password;
    std::string master_log_name;
    unsigned long master_log_pos;
    unsigned int connect_retry;

    MasterInfo() : port(3306), master_log_pos(0), connect_retry(10) {}

    MasterInfo(std::string host_, unsigned int port_, std::string user_,
               std::string password_, unsigned int connect_retry_) :
        host(host_),
        port(port_),
        user(user_),
        password(password_),
        master_log_name(),
        master_log_pos(0),
        connect_retry(connect_retry_)
        {}
};

struct State {
    time_t          connect_time;
    time_t          last_filtered_update;
    time_t          last_event_time;
    time_t          last_update;
    std::string     master_log_name;
    unsigned long   master_log_pos;
    unsigned long   intransaction_pos;
    unsigned int    connect_count;
    bool            state_processing;

    State() :
        connect_time(0),
        last_filtered_update(0),
        last_event_time(0),
        last_update(0),
        master_log_pos(0),
        intransaction_pos(0),
        connect_count(0),
        state_processing(false)
    {}
};

struct ExtStateIface {
    virtual State getState() = 0;
    virtual void setConnecting() = 0;
    virtual time_t getConnectTime() = 0;
    virtual void setLastFilteredUpdateTime() = 0;
    virtual time_t getLastFilteredUpdateTime() = 0;
    virtual void setLastEventTimePos(time_t t, unsigned long pos) = 0;
    virtual time_t getLastUpdateTime() = 0;
    virtual time_t getLastEventTime() = 0;
    virtual unsigned long getIntransactionPos() = 0;
    virtual void setMasterLogNamePos(const std::string& log_name, unsigned long pos) = 0;
    virtual unsigned long getMasterLogPos() = 0;
    virtual std::string getMasterLogName() = 0;

    // Сохраняет master info в persistent хранилище, например, файл или БД.
    // В случае какой-нибудь ошибки, будет пытаться сохранить master info до
    // тех пор, пока не удастся это сделать.
    virtual void saveMasterInfo() = 0;

    // Функция читает master info из persistent хранилища.
    // Если master info не был ранее сохранён (например, первый запуск демона),
    // то библиотека читает бинлоги с текущей позиции.
    // Функция возвращает true, если считана сохранённая позиция, и false - если
    // нет сохранённой позиции (в этом случае функция обнуляет позицию и очищает
    // имя лог-файла).
    // В случае какой-нибудь ошибки чтения из хранилища, функция будет пытаться
    // прочитать снова, пока не сможет прочитать сохранённую позицию или выяснить
    // её отсутствие.
    virtual bool loadMasterInfo(std::string& logname, unsigned long& pos) = 0;

    // Работает так же, как loadMasterInfo(), только записывает в pos последнюю
    // текущую позицию внутри транзакции, если такая есть.
    bool getMasterInfo(std::string& logname, unsigned long& pos)
    {
        unsigned long in_trans_pos = getIntransactionPos();
        std::string master_logname = getMasterLogName();

        if(in_trans_pos!=0 && !master_logname.empty()) {
            logname = master_logname;
            pos = in_trans_pos;
            return true;
        } else
            return loadMasterInfo(logname, pos);
    }
    virtual unsigned int getConnectCount() = 0;
    virtual void setStateProcessing(bool _state) = 0;
    virtual bool getStateProcessing() = 0;
    // Стандартного формата для статистики распределения событий по таблицам нет,
    // поэтому нет функций получения этой статистики.
    virtual void initTableCount(const std::string& t) = 0;
    virtual void incTableCount(const std::string& t) = 0;
};


// Объект-заглушка для ответы на запросы статы через StateHolder, когда libslave
// ещё не проинициализирован
struct EmptyExtState: public ExtStateIface, protected State {
    ~EmptyExtState() {}
    virtual State getState() { return *this; }
    virtual void setConnecting() {}
    virtual time_t getConnectTime() { return 0; }
    virtual void setLastFilteredUpdateTime() {}
    virtual time_t getLastFilteredUpdateTime() { return 0; }
    virtual void setLastEventTimePos(time_t t, unsigned long pos) {}
    virtual time_t getLastUpdateTime() { return 0; }
    virtual time_t getLastEventTime() { return 0; }
    virtual unsigned long getIntransactionPos() { return 0; }
    virtual void setMasterLogNamePos(const std::string& log_name, unsigned long pos) {}
    virtual unsigned long getMasterLogPos() { return 0; }
    virtual std::string getMasterLogName() { return ""; }
    virtual void saveMasterInfo() {}
    virtual bool loadMasterInfo(std::string& logname, unsigned long& pos) { while(true); return false; }
    virtual unsigned int getConnectCount() { return 0; }
    virtual void setStateProcessing(bool _state) {}
    virtual bool getStateProcessing() { return false; }
    virtual void initTableCount(const std::string& t) {}
    virtual void incTableCount(const std::string& t) {}
};

// Предназначен для хранения в синглтоне ExtStateIface или его потомка
struct StateHolder {
    typedef boost::shared_ptr<ExtStateIface> PExtState;
    PExtState ext_state;
    StateHolder() :
        ext_state(new EmptyExtState)
    {}
    ExtStateIface& operator()()
    {
        return *ext_state;
    }
};
}


#endif
