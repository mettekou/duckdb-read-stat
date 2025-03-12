#pragma once

#include "readstat.h"

typedef enum duckdb_read_stat_file_format
{
    DUCKDB_READ_STAT_FILE_FORMAT_SAS,
    DUCKDB_READ_STAT_FILE_FORMAT_SPSS,
    DUCKDB_READ_STAT_FILE_FORMAT_STATA
} duckdb_read_stat_file_format;

typedef enum duckdb_read_stat_datetime_format
{
    DATE_FORMAT_DATE,
    DATE_FORMAT_DATETIME,
    DATE_FORMAT_TIME
} duckdb_read_stat_datetime_format;

typedef struct duckdb_read_stat_bind_data
{
    duckdb_read_stat_file_format file_format;
    const char *format;
    const char *encoding;
    idx_t cardinality;
    const char *path;
    char *error_message;
    duckdb_bind_info bind_info;
} duckdb_read_stat_bind_data;

int duckdb_read_stat_ends_with(const char *string, const char *suffix);

void duckdb_read_stat_bind(duckdb_bind_info info);

int duckdb_read_stat_bind_handle_metadata(readstat_metadata_t *metadata, void *ctx);

int duckdb_read_stat_bind_handle_variable(int index, readstat_variable_t *variable,
                                          const char *val_labels, void *ctx);

void duckdb_read_stat_bind_handle_error(const char *error_message, void *ctx);

typedef struct duckdb_read_stat_init_data
{
    idx_t offset;
    idx_t actual_rows_read;
} duckdb_read_stat_init_data;

void duckdb_read_stat_init(duckdb_init_info info);

typedef struct duckdb_read_stat_context
{
    char *error_message;
    duckdb_function_info function_info;
    duckdb_data_chunk data_chunk;
} duckdb_read_stat_context;

int duckdb_read_stat_handle_value(int obs_index, readstat_variable_t *variable, readstat_value_t value, void *ctx);

void duckdb_read_stat_function(duckdb_function_info info, duckdb_data_chunk output);

void duckdb_read_stat_handle_error(const char *error_message, void *ctx);

void duckdb_read_stat_register_read_stat_function(duckdb_connection connection);

void duckdb_read_stat_replacement_scan(duckdb_replacement_scan_info info, const char *table_name, void *data);