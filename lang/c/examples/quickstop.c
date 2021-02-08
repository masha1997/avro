/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <avro.h>
#include <stdio.h>
#include <stdlib.h>

#ifdef DEFLATE_CODEC
#define QUICKSTOP_CODEC  "deflate"
#else
#define QUICKSTOP_CODEC  "null"
#endif

avro_schema_t pgd_data_map_schema;


void print_array(avro_file_buffer_reader_t reader, avro_datum_t mapping_datumn, char* name)
{
	char* json = NULL;
	avro_datum_t array_mapping, array_chunk;
	avro_record_get(mapping_datumn, name, &array_mapping);

	fprintf(stdout, "\n========================%s========================\n", name);

	for (size_t i = 0; i < (avro_array_size(array_mapping) - 1);)
	{
		avro_datum_t begin_datum, end_datum;
		avro_array_get(array_mapping, i, &begin_datum);
		avro_array_get(array_mapping, ++i, &end_datum);

		int64_t begin, end;

		avro_int64_get(begin_datum, &begin);
		avro_int64_get(end_datum, &end);
		fprintf(stdout, "QUICKSTOP: chunk{[%d] begin: %ld, end: %ld}\n",i, begin, end);
		if (avro_file_reader_subschema_read_chunk(reader, begin, end, name, &array_chunk))
		{
			avro_datum_to_json(array_chunk, 1, &json);
			avro_datum_decref(array_chunk);
			fprintf(stdout, "QUICKSTOP: size[%lu]  : %s\n", avro_array_size(array_chunk), json);
			free(json);
			avro_datum_decref(array_chunk);
		}
	}

	fprintf(stdout, "\n================================================\n");

}

int process_pgd_data()
{
	const char *avro_file = "f_sgeb25868e7855fe7a1d7f446f07923744_14d_v1609459200000_ts1609474609422.avro";
	avro_file_buffer_reader_t db;
	if (avro_file_buffer_reader(avro_file, &db))
	{
		fprintf(stdout, "Error opening file: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}

	avro_datum_t* mapping_datumn;
	char* json;
	create_fields_mapping_block_schema(db, &mapping_datumn);
	avro_datum_to_json(mapping_datumn, 1, &json);
	fprintf(stdout, "QUICKSTOP:  size [%lu] : %s\n", avro_array_size(mapping_datumn), json);
	free(json);

	print_array(db, mapping_datumn, "showings");
	print_array(db, mapping_datumn, "contents");
	print_array(db, mapping_datumn, "collections");

	avro_file_buffer_reader_close(db);
	return 0;
}

int main(void)
{
	process_pgd_data();
	return 0;
}
