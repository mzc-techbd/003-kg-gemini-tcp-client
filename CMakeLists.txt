cmake_minimum_required(VERSION 3.10.0)
project(tcp-client VERSION 0.1.0 LANGUAGES C CXX)

include(FetchContent)

FetchContent_Declare(
  spdlog
  GIT_REPOSITORY https://github.com/gabime/spdlog.git
  GIT_TAG        v1.15.2 # Use a specific stable tag
)
FetchContent_MakeAvailable(spdlog)

add_executable(tcp-client main.cpp)

target_link_libraries(tcp-client PRIVATE spdlog::spdlog)

