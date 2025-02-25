#include <time.h>

#include "duckdb_extension.h"

#include "duckdb_read_stat.h"
#include "duckdb_read_stat_sas.h"

DUCKDB_EXTENSION_EXTERN

int read_stat_bind_handle_metadata(readstat_metadata_t *metadata, void *ctx)
{
    read_stat_bind_data *context = (read_stat_bind_data *)ctx;
    context->cardinality = readstat_get_row_count(metadata);
    return READSTAT_HANDLER_OK;
}

int read_stat_bind_handle_variable(int index, readstat_variable_t *variable,
                                   const char *val_labels, void *ctx)
{
    read_stat_bind_data *context = (read_stat_bind_data *)ctx;
    const char *name = readstat_variable_get_name(variable);
    readstat_type_t type = readstat_variable_get_type(variable);
    const char *format = readstat_variable_get_format(variable);
    duckdb_logical_type logical_type = NULL;

    switch (type)
    {
    case READSTAT_TYPE_STRING:
    case READSTAT_TYPE_STRING_REF:
        logical_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
        break;
    case READSTAT_TYPE_INT8:
        logical_type = duckdb_create_logical_type(DUCKDB_TYPE_TINYINT);
        break;
    case READSTAT_TYPE_INT16:
        logical_type = duckdb_create_logical_type(DUCKDB_TYPE_SMALLINT);
        break;
    case READSTAT_TYPE_INT32:
        logical_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
        break;
    case READSTAT_TYPE_FLOAT:
        logical_type = duckdb_create_logical_type(DUCKDB_TYPE_FLOAT);
        break;
    case READSTAT_TYPE_DOUBLE:
        if (
            // SAS
            !strcmp(format, "WEEKDATE") || !strcmp(format, "MMDDYY") || !strcmp(format, "DDMMYY") || !strcmp(format, "YYMMDD") || !strcmp(format, "DATE") || !strcmp(format, "DATE9") || !strcmp(format, "YYMMDD10") || !strcmp(format, "DDMMYYB") || !strcmp(format, "DDMMYYB10") || !strcmp(format, "DDMMYYC") || !strcmp(format, "DDMMYYC10") || !strcmp(format, "DDMMYYD") || !strcmp(format, "DDMMYYD10") || !strcmp(format, "DDMMYYN6") || !strcmp(format, "DDMMYYN8") || !strcmp(format, "DDMMYYP") || !strcmp(format, "DDMMYYP10") || !strcmp(format, "DDMMYYS") || !strcmp(format, "DDMMYYS10") || !strcmp(format, "MMDDYYB") || !strcmp(format, "MMDDYYB10") || !strcmp(format, "MMDDYYC") || !strcmp(format, "MMDDYYC10") || !strcmp(format, "MMDDYYD") || !strcmp(format, "MMDDYYD10") || !strcmp(format, "MMDDYYN6") || !strcmp(format, "MMDDYYN8") || !strcmp(format, "MMDDYYP") || !strcmp(format, "MMDDYYP10") || !strcmp(format, "MMDDYYS") || !strcmp(format, "MMDDYYS10") || !strcmp(format, "WEEKDATX") || !strcmp(format, "DTDATE") || !strcmp(format, "IS8601DA") || !strcmp(format, "E8601DA") || !strcmp(format, "B8601DA") || !strcmp(format, "YYMMDDB") || !strcmp(format, "YYMMDDD") || !strcmp(format, "YYMMDDN") || !strcmp(format, "YYMMDDP") || !strcmp(format, "YYMMDDS")
            // SPSS without duplicates from SAS
            || !strcmp(format, "DATE8") || !strcmp(format, "DATE11") || !strcmp(format, "DATE12") || !strcmp(format, "ADATE") || !strcmp(format, "ADATE8") || !strcmp(format, "ADATE10") || !strcmp(format, "EDATE") || !strcmp(format, "EDATE8") || !strcmp(format, "EDATE10") || !strcmp(format, "JDATE") || !strcmp(format, "JDATE5") || !strcmp(format, "JDATE7") || !strcmp(format, "SDATE") || !strcmp(format, "SDATE8") || !strcmp(format, "SDATE10")
            // Stata
            || !strcmp(format, "%td") || !strcmp(format, "%d") || !strcmp(format, "%tdD_m_Y") || !strcmp(format, "%tdCCYY-NN-DD"))
        {
            logical_type = duckdb_create_logical_type(DUCKDB_TYPE_DATE);
        }
        else if (
            // SAS
            !strcmp(format, "DATETIME") || !strcmp(format, "DATETIME18") || !strcmp(format, "DATETIME19") || !strcmp(format, "DATETIME20") || !strcmp(format, "DATETIME21") || !strcmp(format, "DATETIME22") || !strcmp(format, "E8601DT") || !strcmp(format, "DATEAMPM") || !strcmp(format, "MDYAMPM") || !strcmp(format, "IS8601DT") || !strcmp(format, "B8601DT") || !strcmp(format, "B8601DN")
            // SPSS without duplicates from SAS
            || !strcmp(format, "DATETIME8") || !strcmp(format, "DATETIME17") || !strcmp(format, "DATETIME23.2") || !strcmp(format, "YMDHMS16") || !strcmp(format, "YMDHMS19") || !strcmp(format, "YMDHMS19.2") || !strcmp(format, "YMDHMS20")
            // Stata
            || !strcmp(format, "%tC") || !strcmp(format, "%tc"))
        {
            logical_type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP);
        }
        else if (
            // SAS
            !strcmp(format, "TIME") || !strcmp(format, "HHMM") || !strcmp(format, "TIME20.3") || !strcmp(format, "TIME20") || !strcmp(format, "TIME5") || !strcmp(format, "TOD") || !strcmp(format, "TIMEAMPM") || !strcmp(format, "IS8601TM") || !strcmp(format, "E8601TM") || !strcmp(format, "B8601TM")
            // SPSS without duplicates from SAS
            || !strcmp(format, "DTIME") || !strcmp(format, "TIME8") || !strcmp(format, "TIME5") || !strcmp(format, "TIME11.2")
            // Stata
            || !strcmp(format, "%tcHH:MM:SS") || !strcmp(format, "%tcHH:MM"))
        {
            logical_type = duckdb_create_logical_type(DUCKDB_TYPE_TIME);
        }
        else
        {
            logical_type = duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
        }
        break;
    }

    duckdb_bind_add_result_column(context->bind_info, name, logical_type);
    duckdb_destroy_logical_type(&logical_type);

    return READSTAT_HANDLER_OK;
}

void read_stat_bind_handle_error(const char *error_message, void *ctx)
{
    read_stat_bind_data *context = (read_stat_bind_data *)ctx;
    context->error_message = error_message;
}

void duckdb_read_stat_init(duckdb_init_info info)
{
    read_stat_init_data *data = duckdb_malloc(sizeof(read_stat_init_data));
    data->offset = 0;
    data->actual_rows_read = 0;
    duckdb_init_set_init_data(info, data, duckdb_free);
}

const struct tm sas_epoch = {.tm_year = 60, .tm_mon = 0, .tm_mday = 1, .tm_zone = "UTC"};
const struct tm spss_epoch = {.tm_year = -318, .tm_mon = 0, .tm_mday = 1, .tm_zone = "UTC"};
const struct tm stata_epoch = {.tm_year = 60, .tm_mon = 0, .tm_mday = 1, .tm_zone = "UTC"};

duckdb_date duckdb_read_stat_convert_date(int days, int seconds, struct tm epoch)
{
    struct tm datetime_struct = {.tm_year = epoch.tm_year, .tm_mon = epoch.tm_mon, .tm_mday = epoch.tm_mday + days, .tm_sec = epoch.tm_sec + seconds, .tm_zone = "UTC"};
    time_t datetime = mktime(&datetime_struct);
    struct tm *datetime_struct_epoch = localtime(&datetime);
    duckdb_date_struct date_struct;
    date_struct.year = datetime_struct_epoch->tm_year + 1900;
    date_struct.month = datetime_struct_epoch->tm_mon + 1;
    date_struct.day = datetime_struct_epoch->tm_mday;
    return duckdb_to_date(date_struct);
}

duckdb_date duckdb_read_stat_to_date(double timestamp, read_stat_file_format file_format)
{
    int days = 0;
    int seconds = 0;

    if (file_format == FILE_FORMAT_SPSS)
    {
        // timestamp is in seconds
        days = (int)(floor(timestamp / 86400));
        seconds = (int)(fmod(timestamp, 86400));
    }
    else
    {
        // timestamp is in days
        days = (int)timestamp;
    }

    switch (file_format)
    {
    case FILE_FORMAT_SAS:
        return duckdb_read_stat_convert_date(days, seconds, sas_epoch);
    case FILE_FORMAT_SPSS:
        return duckdb_read_stat_convert_date(days, seconds, spss_epoch);
    case FILE_FORMAT_STATA:
        return duckdb_read_stat_convert_date(days, seconds, stata_epoch);
    }
}

duckdb_timestamp duckdb_read_stat_convert_timestamp(int days, int seconds, struct tm epoch)
{
    struct tm datetime_struct = {.tm_year = epoch.tm_year, .tm_mon = epoch.tm_mon, .tm_mday = epoch.tm_mday + days, .tm_sec = epoch.tm_sec + seconds};
    time_t datetime = mktime(&datetime_struct);
    struct tm *datetime_struct_epoch = localtime(&datetime);
    duckdb_timestamp_struct timestamp_struct;
    timestamp_struct.date.year = datetime_struct_epoch->tm_year + 1900;
    timestamp_struct.date.month = datetime_struct_epoch->tm_mon + 1;
    timestamp_struct.date.day = datetime_struct_epoch->tm_mday;
    timestamp_struct.time.hour = datetime_struct_epoch->tm_hour;
    timestamp_struct.time.min = datetime_struct_epoch->tm_min;
    timestamp_struct.time.sec = datetime_struct_epoch->tm_sec;
    timestamp_struct.time.micros = 0;
    return duckdb_to_timestamp(timestamp_struct);
}

duckdb_timestamp duckdb_read_stat_to_timestamp(double timestamp, read_stat_file_format file_format)
{
    int days = 0;
    int msecs = 0;
    int seconds = 0;
    int usecs = 0;
    if (file_format == FILE_FORMAT_STATA)
    {
        // tstamp is in millisecons
        days = (int)(floor(timestamp / 86400000));
        msecs = fmod(timestamp, 86400000);
        seconds = (int)(msecs / 1000);
        usecs = (int)(fmod(msecs, 1000) * 1000);
    }
    else
    {
        // tstamp in seconds
        days = (int)(floor(timestamp / 86400));
        seconds = (int)(fmod(timestamp, 86400));
    }
    switch (file_format)
    {
    case FILE_FORMAT_SAS:
        return duckdb_read_stat_convert_timestamp(days, seconds, sas_epoch);
    case FILE_FORMAT_SPSS:
        return duckdb_read_stat_convert_timestamp(days, seconds, spss_epoch);
    case FILE_FORMAT_STATA:
        return duckdb_read_stat_convert_timestamp(days, seconds, stata_epoch);
    }
}

const int seconds_in_hour = 60 * 60;
const int seconds_in_minute = 60;

duckdb_time duckdb_read_stat_convert_time(int days, int seconds)
{
    int hours, minutes, remaining_seconds;
    int total = (24 * 3600 * days) + seconds;

    hours = (total / seconds_in_hour);
    remaining_seconds = total - (hours * seconds_in_hour);
    minutes = remaining_seconds / seconds_in_minute;
    remaining_seconds = remaining_seconds - (minutes * seconds_in_minute);

    duckdb_time_struct time_struct;

    time_struct.hour = hours;
    time_struct.min = minutes;
    time_struct.sec = remaining_seconds;
    time_struct.micros = 0;

    return duckdb_to_time(time_struct);
}

duckdb_time duckdb_read_stat_to_time(double tstamp, read_stat_file_format file_format)
{
    int days = 0;
    int seconds = 0;
    double msecs = 0;
    int usecs = 0;

    if (file_format == FILE_FORMAT_STATA)
    {
        // tstamp is in millisecons
        days = (int)(floor(tstamp / 86400000));
        msecs = fmod(tstamp, 86400000);
        seconds = (int)(msecs / 1000);
        usecs = (int)(fmod(msecs, 1000) * 1000);
    }
    else
    {
        // tstamp in seconds
        days = (int)(floor(tstamp / 86400));
        seconds = (int)(fmod(tstamp, 86400));
    }

    return duckdb_read_stat_convert_time(days, seconds);
}

int read_stat_handle_value(int obs_index, readstat_variable_t *variable, readstat_value_t value, void *ctx)
{
    read_stat_context *context = (read_stat_context *)ctx;
    read_stat_bind_data *bind_data = duckdb_function_get_bind_data(context->function_info);
    read_stat_init_data *init_data = duckdb_function_get_init_data(context->function_info);
    idx_t var_index = readstat_variable_get_index(variable);
    readstat_type_t type = readstat_value_type(value);
    const char *format = readstat_variable_get_format(variable);
    duckdb_vector vector = duckdb_data_chunk_get_vector(context->data_chunk, var_index);

    if (!readstat_value_is_system_missing(value) /*&& !readstat_value_is_tagged_missing(value)*/)
    {
        switch (type)
        {
        case READSTAT_TYPE_STRING:
            const char *string_value = readstat_string_value(value);
            duckdb_vector_assign_string_element(vector, obs_index, string_value);
            break;

        case READSTAT_TYPE_INT8:
            int8_t *output_value_int8 = (int8_t *)duckdb_vector_get_data(vector);
            output_value_int8[obs_index] = readstat_int8_value(value);
            break;

        case READSTAT_TYPE_INT16:
            int16_t *output_value_int16 = (int16_t *)duckdb_vector_get_data(vector);
            output_value_int16[obs_index] = readstat_int16_value(value);
            break;

        case READSTAT_TYPE_INT32:
            int32_t *output_value_int32 = (int32_t *)duckdb_vector_get_data(vector);
            output_value_int32[obs_index] = readstat_int32_value(value);
            break;

        case READSTAT_TYPE_FLOAT:
            float *output_value_float = (float *)duckdb_vector_get_data(vector);
            output_value_float[obs_index] = readstat_float_value(value);
            break;

        case READSTAT_TYPE_DOUBLE:
            double double_value = readstat_double_value(value);
            if (
                // SAS
                !strcmp(format, "WEEKDATE") || !strcmp(format, "MMDDYY") || !strcmp(format, "DDMMYY") || !strcmp(format, "YYMMDD") || !strcmp(format, "DATE") || !strcmp(format, "DATE9") || !strcmp(format, "YYMMDD10") || !strcmp(format, "DDMMYYB") || !strcmp(format, "DDMMYYB10") || !strcmp(format, "DDMMYYC") || !strcmp(format, "DDMMYYC10") || !strcmp(format, "DDMMYYD") || !strcmp(format, "DDMMYYD10") || !strcmp(format, "DDMMYYN6") || !strcmp(format, "DDMMYYN8") || !strcmp(format, "DDMMYYP") || !strcmp(format, "DDMMYYP10") || !strcmp(format, "DDMMYYS") || !strcmp(format, "DDMMYYS10") || !strcmp(format, "MMDDYYB") || !strcmp(format, "MMDDYYB10") || !strcmp(format, "MMDDYYC") || !strcmp(format, "MMDDYYC10") || !strcmp(format, "MMDDYYD") || !strcmp(format, "MMDDYYD10") || !strcmp(format, "MMDDYYN6") || !strcmp(format, "MMDDYYN8") || !strcmp(format, "MMDDYYP") || !strcmp(format, "MMDDYYP10") || !strcmp(format, "MMDDYYS") || !strcmp(format, "MMDDYYS10") || !strcmp(format, "WEEKDATX") || !strcmp(format, "DTDATE") || !strcmp(format, "IS8601DA") || !strcmp(format, "E8601DA") || !strcmp(format, "B8601DA") || !strcmp(format, "YYMMDDB") || !strcmp(format, "YYMMDDD") || !strcmp(format, "YYMMDDN") || !strcmp(format, "YYMMDDP") || !strcmp(format, "YYMMDDS")
                // SPSS without duplicates from SAS
                || !strcmp(format, "DATE8") || !strcmp(format, "DATE11") || !strcmp(format, "DATE12") || !strcmp(format, "ADATE") || !strcmp(format, "ADATE8") || !strcmp(format, "ADATE10") || !strcmp(format, "EDATE") || !strcmp(format, "EDATE8") || !strcmp(format, "EDATE10") || !strcmp(format, "JDATE") || !strcmp(format, "JDATE5") || !strcmp(format, "JDATE7") || !strcmp(format, "SDATE") || !strcmp(format, "SDATE8") || !strcmp(format, "SDATE10")
                // Stata
                || !strcmp(format, "%td") || !strcmp(format, "%d") || !strcmp(format, "%tdD_m_Y") || !strcmp(format, "%tdCCYY-NN-DD"))
            {
                duckdb_date converted = duckdb_read_stat_to_date(double_value, bind_data->file_format);
                duckdb_date *output_value_timestamp = (duckdb_date *)duckdb_vector_get_data(vector);
                output_value_timestamp[obs_index] = converted;
            }
            else if (
                // SAS
                !strcmp(format, "DATETIME") || !strcmp(format, "DATETIME18") || !strcmp(format, "DATETIME19") || !strcmp(format, "DATETIME20") || !strcmp(format, "DATETIME21") || !strcmp(format, "DATETIME22") || !strcmp(format, "E8601DT") || !strcmp(format, "DATEAMPM") || !strcmp(format, "MDYAMPM") || !strcmp(format, "IS8601DT") || !strcmp(format, "B8601DT") || !strcmp(format, "B8601DN")
                // SPSS without duplicates from SAS
                || !strcmp(format, "DATETIME8") || !strcmp(format, "DATETIME17") || !strcmp(format, "DATETIME23.2") || !strcmp(format, "YMDHMS16") || !strcmp(format, "YMDHMS19") || !strcmp(format, "YMDHMS19.2") || !strcmp(format, "YMDHMS20")
                // Stata
                || !strcmp(format, "%tC") || !strcmp(format, "%tc"))
            {
                duckdb_timestamp converted = duckdb_read_stat_to_timestamp(double_value, bind_data->file_format);
                duckdb_timestamp *output_value_timestamp = (duckdb_timestamp *)duckdb_vector_get_data(vector);
                output_value_timestamp[obs_index] = converted;
            }
            else if (
                // SAS
                !strcmp(format, "TIME") || !strcmp(format, "HHMM") || !strcmp(format, "TIME20.3") || !strcmp(format, "TIME20") || !strcmp(format, "TIME5") || !strcmp(format, "TOD") || !strcmp(format, "TIMEAMPM") || !strcmp(format, "IS8601TM") || !strcmp(format, "E8601TM") || !strcmp(format, "B8601TM")
                // SPSS without duplicates from SAS
                || !strcmp(format, "DTIME") || !strcmp(format, "TIME8") || !strcmp(format, "TIME5") || !strcmp(format, "TIME11.2"))
            {
                duckdb_time converted = duckdb_read_stat_to_time(double_value, bind_data->file_format);
                duckdb_time *output_value_timestamp = (duckdb_time *)duckdb_vector_get_data(vector);
                output_value_timestamp[obs_index] = converted;
            }
            else
            {
                double *output_value = (double *)duckdb_vector_get_data(vector);
                output_value[obs_index] = double_value;
            }
            break;
        }
    }
    else
    {
        duckdb_vector_ensure_validity_writable(vector);
        int64_t *validity = duckdb_vector_get_validity(vector);
        duckdb_validity_set_row_invalid(validity, obs_index);
    }

    init_data->actual_rows_read = obs_index + 1;

    return READSTAT_HANDLER_OK;
}

int HandleValueLabel(const char *val_labels, readstat_value_t value, const char *label, void *ctx)
{
}

void read_stat_handle_error(const char *error_message, void *ctx)
{
    read_stat_context *context = (read_stat_context *)ctx;
    context->error_message = error_message;
}

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection, duckdb_extension_info info, struct duckdb_extension_access *access)
{
    duckdb_read_stat_sas_register_read_sas7bdat_function(connection);
    duckdb_add_replacement_scan(*(access->get_database(info)), duckdb_read_stat_sas_read_sas7bdat_replacement_scan, NULL, NULL);
    return true;
}
