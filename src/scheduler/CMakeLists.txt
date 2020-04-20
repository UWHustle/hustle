add_library(
        scheduler
        Channel.h Channel.cpp
        Block.h Block.cpp
)

target_link_libraries(scheduler
        nng
)
