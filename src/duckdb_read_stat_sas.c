#include "duckdb_extension.h"

#include "readstat.h"

#include "duckdb_read_stat.h"
#include "duckdb_read_stat_sas.h"

DUCKDB_EXTENSION_EXTERN

void read_sas_bind(duckdb_bind_info info)
{
	readstat_parser_t *parser = readstat_parser_init();
	duckdb_value path_value = duckdb_bind_get_parameter(info, 0);
	const char *path = duckdb_get_varchar(path_value);
	read_stat_bind_data *data = duckdb_malloc(sizeof(read_stat_bind_data));

	data->file_format = FILE_FORMAT_SAS;
	data->bind_info = info;
	data->path = path;
	readstat_set_metadata_handler(parser, &read_stat_bind_handle_metadata);
	readstat_set_variable_handler(parser, &read_stat_bind_handle_variable);
	readstat_set_error_handler(parser, &read_stat_bind_handle_error);
	readstat_set_row_limit(parser, 0);

	if (readstat_parse_sas7bdat(parser, path, data) != READSTAT_OK)
	{
		duckdb_bind_set_error(info, data->error_message);
		readstat_parser_free(parser);
		return;
	}

	duckdb_bind_set_cardinality(data->bind_info, data->cardinality, true);
	duckdb_bind_set_bind_data(info, data, duckdb_free);
	readstat_parser_free(parser);
}

void read_sas(duckdb_function_info info, duckdb_data_chunk output)
{
	read_stat_init_data *init_data = (read_stat_init_data *)duckdb_function_get_init_data(info);
	read_stat_bind_data *bind_data = (read_stat_bind_data *)duckdb_function_get_bind_data(info);

	if (init_data->offset >= bind_data->cardinality)
	{
		duckdb_data_chunk_set_size(output, 0);
		return;
	}

	readstat_error_t error = READSTAT_OK;
	readstat_parser_t *parser = readstat_parser_init();

	readstat_set_row_offset(parser, (long)init_data->offset);
	readstat_set_row_limit(parser, (long)2);
	readstat_set_value_handler(parser, &read_stat_handle_value);
	readstat_set_error_handler(parser, &read_stat_handle_error);

	read_stat_context *context = duckdb_malloc(sizeof(read_stat_context));
	context->function_info = info;
	context->data_chunk = output;
	error = readstat_parse_sas7bdat(parser, bind_data->path, context);
	readstat_parser_free(parser);

	if (error != READSTAT_OK)
	{
		duckdb_function_set_error(info, context->error_message);
	}

	init_data->offset += init_data->actual_rows_read;
	duckdb_data_chunk_set_size(output, init_data->actual_rows_read);
	init_data->actual_rows_read = 0;
	duckdb_free(context);
}

int duckdb_read_stat_ends_with(const char *string, const char *suffix)
{
	if (string == NULL || suffix == NULL)
		return false;
	size_t lenstr = strlen(string);
	size_t lensuffix = strlen(suffix);

	return lensuffix > lenstr ? false : strncmp(string + lenstr - lensuffix, suffix, lensuffix) == 0;
}

void duckdb_read_stat_sas_read_sas7bdat_replacement_scan(duckdb_replacement_scan_info info, const char *table_name, void *data)
{
	if (duckdb_read_stat_ends_with(table_name, ".sas7bdat"))
	{
		duckdb_replacement_scan_set_function_name(info, "read_sas7bdat");
		duckdb_replacement_scan_add_parameter(info, duckdb_create_varchar(table_name));
	}
}

void duckdb_read_stat_sas_register_read_sas7bdat_function(duckdb_connection connection)
{
	duckdb_table_function function = duckdb_create_table_function();
	duckdb_table_function_set_name(function, "read_sas7bdat");

	duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_table_function_add_parameter(function, varchar_type);
	// duckdb_table_function_add_named_parameter(function, "encoding", varchar_type);
	duckdb_destroy_logical_type(&varchar_type);

	duckdb_table_function_set_bind(function, &read_sas_bind);
	duckdb_table_function_set_init(function, &duckdb_read_stat_init);
	duckdb_table_function_set_function(function, &read_sas);

	duckdb_state result = duckdb_register_table_function(connection, function);

	duckdb_destroy_table_function(&function);
}