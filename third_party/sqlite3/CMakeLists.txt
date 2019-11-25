add_library(
        sqlite3
        sqlite3.c
	sqlite3.h
)

target_link_libraries(sqlite3 ${CMAKE_DL_LIBS})
