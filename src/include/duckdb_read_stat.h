#pragma once

#include "readstat.h"

typedef enum read_stat_file_format
{
    FILE_FORMAT_SAS,
    FILE_FORMAT_SPSS,
    FILE_FORMAT_STATA
} read_stat_file_format;

typedef enum read_stat_datetime_format
{
    DATE_FORMAT_DATE,
    DATE_FORMAT_DATETIME,
    DATE_FORMAT_TIME
} read_stat_datetime_format;

typedef struct read_stat_bind_data
{
    read_stat_file_format file_format;
    idx_t cardinality;
    const char *path;
    const char *error_message;
    duckdb_bind_info bind_info;
} read_stat_bind_data;

int read_stat_bind_handle_metadata(readstat_metadata_t *metadata, void *ctx);

int read_stat_bind_handle_variable(int index, readstat_variable_t *variable,
                                   const char *val_labels, void *ctx);

void read_stat_bind_handle_error(const char *error_message, void *ctx);

typedef struct read_stat_init_data
{
    idx_t offset;
    idx_t actual_rows_read;
} read_stat_init_data;

void duckdb_read_stat_init(duckdb_init_info info);

typedef struct read_stat_context
{
    const char *error_message;
    duckdb_function_info function_info;
    duckdb_data_chunk data_chunk;
} read_stat_context;

int read_stat_handle_value(int obs_index, readstat_variable_t *variable, readstat_value_t value, void *ctx);

void read_stat_handle_error(const char *error_message, void *ctx);