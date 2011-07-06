
# Please specify the directories with boost and mysql include files here.
BOOST_INCLUDES = /begun/daemon/include
MYSQL_INCLUDES = /begun/daemon/include/mysql

CXX = g++
CFLAGS = -I$(BOOST_INCLUDES) -I$(MYSQL_INCLUDES) -O3 -finline-functions -Wno-inline -Wall -pthread
LFLAGS = -L/begun/daemon/lib64/mysql -lmysqlclient_r

IDEPS = Logging.h Slave.h SlaveStats.h field.h nanomysql.h recordset.h relayloginfo.h slave_log_event.h table.h collate.h
OBJS = Slave.o field.o slave_log_event.o collate.o

STATIC_LIB = libslave.a
SHARED_LIB = libslave.so

all: $(STATIC_LIB) $(SHARED_LIB)

%.o: %.cpp $(IDEPS)
	$(CXX) $(CFLAGS) $< -fPIC -c -o $@

$(STATIC_LIB): $(OBJS)
	-rm -f $(STATIC_LIB)
	ar rc $(STATIC_LIB) $(OBJS)

$(SHARED_LIB): $(OBJS)
	$(CXX) $(LFLAGS) $(OBJS) -shared -o $(SHARED_LIB)

clean:
	-rm -f $(OBJS) $(STATIC_LIB) $(SHARED_LIB) 


test.out: test/test.cpp $(IDEPS) $(STATIC_LIB)
	$(CXX) $(CFLAGS) -I. test/test.cpp $(STATIC_LIB) $(LFLAGS) -o test.out

test: test.out


