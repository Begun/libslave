CMAKE_MINIMUM_REQUIRED (VERSION 2.8)

PROJECT (libslave CXX)

SET (SLAVE_VERSION 0)

SET (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

ADD_DEFINITIONS (-O3 -finline-functions -Wno-inline -Wall -pthread -fPIC)

OPTION (ENABLE_STATIC "Build static libslave" ON)
OPTION (ENABLE_TEST "Test binary" ON)

FIND_PATH (MYSQL_INCLUDE_DIR mysql/mysql.h
		$ENV{MYSQL_INCLUDE_DIR}
		$ENV{MYSQL_DIR}/include
		/usr/include/mysql
		/usr/local/include/mysql
		/opt/mysql/mysql/include
		/opt/mysql/mysql/include/mysql
		/opt/mysql/include
		/opt/local/include/mysql5
		/usr/local/mysql/include
		/usr/local/mysql/include/mysql
		$ENV{ProgramFiles}/MySQL/*/include
		$ENV{SystemDrive}/MySQL/*/include)

FIND_LIBRARY (MYSQL_LIB NAMES mysqlclient_r
		 PATHS
		 $ENV{MYSQL_DIR}/libmysql_r/.libs
		 $ENV{MYSQL_DIR}/lib
		 $ENV{MYSQL_DIR}/lib/mysql
		 /usr/lib/mysql
		 /usr/local/lib/mysql
		 /usr/local/mysql/lib
		 /usr/local/mysql/lib/mysql
		 /opt/local/mysql5/lib
		 /opt/local/lib/mysql5/mysql
		 /opt/mysql/mysql/lib/mysql
		 /opt/mysql/lib/mysql)

IF (MYSQL_LIB)
	GET_FILENAME_COMPONENT (MYSQL_LIB_DIR ${MYSQL_LIB} PATH)
ENDIF (MYSQL_LIB)

IF (MYSQL_INCLUDE_DIR AND MYSQL_LIB_DIR)
	SET(MYSQL_FOUND TRUE)

	INCLUDE_DIRECTORIES (${MYSQL_INCLUDE_DIR})
	LINK_DIRECTORIES (${MYSQL_LIB_DIR})

	SET(MYSQL_CLIENT_LIBS mysqlclient_r)

ELSE (MYSQL_INCLUDE_DIR AND MYSQL_LIB_DIR)
	MESSAGE(FATAL_ERROR "Cannot find MySQL. Include dir: ${MYSQL_INCLUDE_DIR}  library dir: ${MYSQL_LIB_DIR}")
ENDIF (MYSQL_INCLUDE_DIR AND MYSQL_LIB_DIR)

FIND_PACKAGE (Boost 1.40.0 REQUIRED COMPONENTS
	thread
	system)

set(SOURCES
	Slave.cpp
	collate.cpp
	field.cpp
	slave_log_event.cpp)

set(HEADERS
	Logging.h
	Slave.h
	SlaveStats.h
	collate.h
	field.h
	nanomysql.h
	recordset.h
	relayloginfo.h
	slave_log_event.h
	table.h)

INCLUDE_DIRECTORIES (
	${MYSQL_INCLUDE_DIR}/mysql
	${Boost_INCLUDE_DIRS})

LINK_DIRECTORIES (
	${MYSQL_LIB_DIR}
	${Boost_LIBRARY_DIRS})

IF (ENABLE_STATIC)
	ADD_LIBRARY (slave-st STATIC ${SOURCES})
	SET_TARGET_PROPERTIES(slave-st PROPERTIES OUTPUT_NAME slave)
	TARGET_LINK_LIBRARIES (slave-st
		${MYSQL_CLIENT_LIBS})
	INSTALL(TARGETS slave-st
		DESTINATION lib
		PERMISSIONS OWNER_READ GROUP_READ WORLD_READ)
ENDIF (ENABLE_STATIC)

ADD_LIBRARY (slave SHARED ${SOURCES})

SET_TARGET_PROPERTIES (slave PROPERTIES
	VERSION ${SLAVE_VERSION})

TARGET_LINK_LIBRARIES (slave
	${MYSQL_CLIENT_LIBS})

IF (ENABLE_TEST)
	INCLUDE_DIRECTORIES ("${CMAKE_SOURCE_DIR}")
	ADD_EXECUTABLE (slave_test
		${MYSQL_INCLUDE_DIR}/mysql
		test/test.cpp)
	TARGET_LINK_LIBRARIES (slave_test
		pthread
		slave)
	INSTALL (TARGETS slave_test
		RUNTIME DESTINATION bin COMPONENT runtime)
ENDIF (ENABLE_TEST)

INSTALL (FILES ${HEADERS}
	DESTINATION "include/libslave")

INSTALL (TARGETS slave
	LIBRARY DESTINATION lib COMPONENT runtime
	ARCHIVE DESTINATION lib COMPONENT development
	PERMISSIONS OWNER_READ GROUP_READ WORLD_READ)
