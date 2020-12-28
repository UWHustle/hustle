add_library(
        sqlite3
        sqlite3.c
		sqlite3.h
)
set(CMAKE_CXX_FLAGS "-DSQLITE_THREADSAFE=0")

target_link_libraries(sqlite3 PUBLIC ${CMAKE_DL_LIBS} hustle_src_resolver hustle_src_storage)
