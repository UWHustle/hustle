add_library(
        sqlite3
        sqlite3.c
		sqlite3.h
)

target_link_libraries(sqlite3 PUBLIC ${CMAKE_DL_LIBS} hustle_src_resolver hustle_src_storage)
