cmake_minimum_required(VERSION 3.15...3.31)
option(DUCKDB_WASM_EXTENSIONS "Whether compiling for Wasm target" OFF)

set(EXTENSION_NAME read_stat)

###
# Configuration
###
if(NOT DEFINED EXTENSION_NAME)
    message(FATAL_ERROR "DuckDB extension name is required")
endif()
if (DEFINED TARGET_DUCKDB_VERSION_MAJOR)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_MAJOR=${TARGET_DUCKDB_VERSION_MAJOR})
endif()
if (DEFINED TARGET_DUCKDB_VERSION_MINOR)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_MINOR=${TARGET_DUCKDB_VERSION_MINOR})
endif()
if (DEFINED TARGET_DUCKDB_VERSION_PATCH)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_PATCH=${TARGET_DUCKDB_VERSION_PATCH})
endif()

add_definitions(-DDUCKDB_EXTENSION_NAME=${EXTENSION_NAME})

if (DEFINED DUCKDB_EXTENSION_API_VERSION_UNSTABLE)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_UNSTABLE=${DUCKDB_EXTENSION_API_VERSION_UNSTABLE})
endif()

###
# Build
###
project(${EXTENSION_NAME} LANGUAGES C)

# Create Extension library
set(EXTENSION_SOURCES
        src/duckdb_read_stat.c
        third_party/readstat/CKHashTable.c
        third_party/readstat/readstat_bits.c
        third_party/readstat/readstat_convert.c
        third_party/readstat/readstat_error.c
        third_party/readstat/readstat_io_unistd.c
        third_party/readstat/readstat_malloc.c
        third_party/readstat/readstat_metadata.c
        third_party/readstat/readstat_parser.c
        third_party/readstat/readstat_value.c
        third_party/readstat/readstat_variable.c
        third_party/readstat/readstat_writer.c
        third_party/readstat/sas/readstat_sas.c
        third_party/readstat/sas/readstat_sas_rle.c
        third_party/readstat/sas/readstat_sas7bdat_read.c
        third_party/readstat/spss/readstat_sav_compress.c
        third_party/readstat/spss/readstat_sav_parse_timestamp.c
        third_party/readstat/spss/readstat_sav_parse.c
        third_party/readstat/spss/readstat_sav_read.c
        third_party/readstat/spss/readstat_sav_write.c
        third_party/readstat/spss/readstat_sav.c
        third_party/readstat/spss/readstat_spss_parse.c
        third_party/readstat/spss/readstat_spss.c
        third_party/readstat/spss/readstat_zsav_compress.c
        third_party/readstat/spss/readstat_zsav_read.c
        third_party/readstat/spss/readstat_zsav_write.c
        third_party/readstat/stata/readstat_dta.c
        third_party/readstat/stata/readstat_dta_parse_timestamp.c
        third_party/readstat/stata/readstat_dta_read.c
)

if (DUCKDB_WASM_EXTENSION)
	add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})
else()
	add_library(${EXTENSION_NAME} SHARED ${EXTENSION_SOURCES})
endif()

# Hide symbols
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_C_VISIBILITY_PRESET hidden)
set(VISIBILITY_INLINES_HIDDEN ON)
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -s")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -s")

set(CMAKE_C_FLAGS  "${CMAKE_C_FLAGS} -fPIC -D HAVE_ZLIB=1")

add_subdirectory(third_party/zlib)
target_link_libraries(${EXTENSION_NAME} PRIVATE zlibstatic)
target_include_directories(${EXTENSION_NAME} PRIVATE
    "${PROJECT_BINARY_DIR}"
    "${PROJECT_SOURCE_DIR}/third_party/zlib"
)

# Include own headers
target_include_directories(${EXTENSION_NAME} PRIVATE src/include)

# Include DuckDB C API headers
target_include_directories(${EXTENSION_NAME} PRIVATE duckdb_capi)

target_include_directories(${EXTENSION_NAME} PRIVATE third_party/readstat)