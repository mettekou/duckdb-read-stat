#pragma once

#include "duckdb_extension.h"

void duckdb_read_stat_sas_read_sas7bdat_replacement_scan(duckdb_replacement_scan_info info, const char *table_name, void *data);
void duckdb_read_stat_sas_register_read_sas7bdat_function(duckdb_connection connection);