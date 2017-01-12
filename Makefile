
# Please specify the directories with boost and mysql include files here.
PREFIX=/usr
BOOST_INCLUDES = ${PREFIX}/include
MYSQL_INCLUDES = ${PREFIX}/include/mysql

CXX = g++
CFLAGS = -I$(BOOST_INCLUDES) -I$(MYSQL_INCLUDES) -O3 -finline-functions -Wno-inline -Wall -pthread
LFLAGS = -L${PREFIX}/lib64/mysql -lmysqlclient_r

IDEPS = Logging.h Slave.h SlaveStats.h field.h nanomysql.h nanofield.h recordset.h relayloginfo.h slave_log_event.h table.h collate.h
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

unit_test.out: test/unit_test.cpp $(IDEPS) $(STATIC_LIB)
	$(CXX) $(CFLAGS) -I. test/unit_test.cpp $(STATIC_LIB) $(LFLAGS) -lboost_unit_test_framework-mt -lboost_thread-mt -o unit_test.out

unit_test: unit_test.out
