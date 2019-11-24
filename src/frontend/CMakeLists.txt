add_library(
        hustle_frontend
        HustleFrontend.cpp HustleFrontend.h
)

target_link_libraries(
        hustle_frontend
        sqlite3
        absl::strings
)