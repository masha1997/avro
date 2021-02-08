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

#include "avro_private.h"
#include "avro/errors.h"
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include "encoding.h"
#include "schema.h"

#define DEFAULT_CHUNK_SIZE	512

static int skip_array(avro_reader_t reader, const avro_encoding_t * enc,
                      struct avro_array_schema_t *writers_schema, avro_datum_t* mapped_datum)
{
    int rval;
    int64_t i;
    int64_t block_count;
    int64_t block_size;

    int chunk_size = DEFAULT_CHUNK_SIZE;

    check_prefix(rval, enc->read_long(reader, &block_count),
                 "Cannot read array block count: ");

    while (block_count != 0) {
        if (block_count < 0) {
            block_count = block_count * -1;
            check_prefix(rval, enc->read_long(reader, &block_size),
                         "Cannot read array block size: ");
        }

        for (i = 0; i < block_count; i++) {
            if (!(i % chunk_size) && mapped_datum)
            {
                avro_datum_t current_position = avro_int64(avro_reader_file_ftell(reader));
                check_prefix(rval,
                             avro_array_append_datum(mapped_datum, current_position),
                             "Cannot write begin offset to mapped schema: ");
                avro_datum_decref(current_position);
            }
            check_prefix(rval, avro_skip_data(reader, writers_schema->items),
                         "Cannot skip array element: ");
        }
        /* To populate end-offset of the last chunk in block*/
        if (block_count % chunk_size && mapped_datum)
        {
            avro_datum_t current_position = avro_int64(avro_reader_file_ftell(reader));
            check_prefix(rval,
                         avro_array_append_datum(mapped_datum, current_position),
                         "Cannot write begin offset to mapped schema: ");
            avro_datum_decref(current_position);
        }

        check_prefix(rval, enc->read_long(reader, &block_count),
                     "Cannot read array block count: ");
    }
    return 0;
}

static int skip_map(avro_reader_t reader, const avro_encoding_t * enc,
                    struct avro_map_schema_t *writers_schema)
{
    int rval;
    int64_t i, block_count;

    check_prefix(rval, enc->read_long(reader, &block_count),
                 "Cannot read map block count: ");
    while (block_count != 0) {
        int64_t block_size;
        if (block_count < 0) {
            block_count = block_count * -1;
            check_prefix(rval, enc->read_long(reader, &block_size),
                         "Cannot read map block size: ");
        }
        for (i = 0; i < block_count; i++) {
            check_prefix(rval, enc->skip_string(reader),
                         "Cannot skip map key: ");
            check_prefix(rval,
                         avro_skip_data(reader,
                                        avro_schema_to_map(writers_schema)->
                                                values),
                         "Cannot skip map value: ");
        }
        check_prefix(rval, enc->read_long(reader, &block_count),
                     "Cannot read map block count: ");
    }
    return 0;
}

static int skip_union(avro_reader_t reader, const avro_encoding_t * enc,
                      struct avro_union_schema_t *writers_schema)
{
    int rval;
    int64_t index;
    avro_schema_t branch_schema;

    check_prefix(rval, enc->read_long(reader, &index),
                 "Cannot read union discriminant: ");
    branch_schema = avro_schema_union_branch(&writers_schema->obj, index);
    if (!branch_schema) {
        return EILSEQ;
    }
    return avro_skip_data(reader, branch_schema);
}

static int skip_record(avro_reader_t reader, const avro_encoding_t * enc,
                       struct avro_record_schema_t *writers_schema, avro_datum_t* mapped_datum)
{
    int rval;
    long i;

    AVRO_UNUSED(enc);

    for (i = 0; i < writers_schema->fields->num_entries; i++) {
        avro_schema_t field_schema;
        field_schema = avro_schema_record_field_get_by_index
                (&writers_schema->obj, i);
        if (mapped_datum) {
            avro_datum_t* mapped_field_datum = NULL;
            check_prefix(rval,
                         avro_map_data(reader, field_schema, &mapped_field_datum),
                         "Cannot map record field offset: ");
            if (mapped_field_datum) {
                check_prefix(rval,
                             avro_schema_record_field_append(avro_datum_get_schema(mapped_datum),
                                                             avro_schema_record_field_name(&writers_schema->obj, i),
                                                             avro_datum_get_schema(mapped_field_datum)),
                             "Cannot set the record field");
                check_prefix(rval,
                             avro_record_set(mapped_datum, avro_schema_record_field_name(&writers_schema->obj, i), mapped_field_datum),
                             "Cannot set the record field");
                avro_datum_decref(mapped_field_datum);
            }
        } else {
            check_prefix(rval, avro_skip_data(reader, field_schema), "Cannot skip record field: ");
        }
    }
    return 0;
}

int avro_map_data(avro_reader_t reader, avro_schema_t writers_schema, avro_datum_t** mapped_datum)
{
    check_param(EINVAL, reader, "reader");
    check_param(EINVAL, is_avro_schema(writers_schema), "writer schema");

    int rval = EINVAL;
    const avro_encoding_t *enc = &avro_binary_encoding;

    switch (avro_typeof(writers_schema)) {
        case AVRO_ARRAY:
            *mapped_datum = avro_array(avro_schema_long());

            check_prefix(rval, skip_array(reader, enc,
                                          avro_schema_to_array(writers_schema), *mapped_datum),
                         "Cannot skip data");

            break;

        case AVRO_RECORD:
            *mapped_datum = avro_record(avro_schema_record(avro_schema_name(writers_schema),
                                                           avro_schema_namespace(writers_schema)));
            check_prefix(rval,
                         avro_schema_record_field_append(avro_datum_get_schema(*mapped_datum), "__offset", avro_schema_long()),
                         "Cannot append field to record schema");
            check_prefix(rval,
                         avro_record_set(*mapped_datum, "__offset", avro_int64(avro_reader_file_ftell(reader))),
                         "Cannot set offset to record datum");

            rval = skip_record(reader, enc,
                               avro_schema_to_record(writers_schema), *mapped_datum);
            break;
        default:
            rval = avro_skip_data(reader, writers_schema);
    }

    return rval;
}

int avro_skip_data(avro_reader_t reader, avro_schema_t writers_schema)
{
    check_param(EINVAL, reader, "reader");
    check_param(EINVAL, is_avro_schema(writers_schema), "writer schema");

    int rval = EINVAL;
    const avro_encoding_t *enc = &avro_binary_encoding;

    switch (avro_typeof(writers_schema)) {
        case AVRO_NULL:
            rval = enc->skip_null(reader);
            break;

        case AVRO_BOOLEAN:
            rval = enc->skip_boolean(reader);
            break;

        case AVRO_STRING:
            rval = enc->skip_string(reader);
            break;

        case AVRO_INT32:
            rval = enc->skip_int(reader);
            break;

        case AVRO_INT64:
            rval = enc->skip_long(reader);
            break;

        case AVRO_FLOAT:
            rval = enc->skip_float(reader);
            break;

        case AVRO_DOUBLE:
            rval = enc->skip_double(reader);
            break;

        case AVRO_BYTES:
            rval = enc->skip_bytes(reader);
            break;

        case AVRO_FIXED:
            rval =
                    avro_skip(reader,
                              avro_schema_to_fixed(writers_schema)->size);
            break;

        case AVRO_ENUM:
            rval = enc->skip_long(reader);
            break;

        case AVRO_ARRAY:
            rval =
                    skip_array(reader, enc,
                               avro_schema_to_array(writers_schema), NULL);
            break;

        case AVRO_MAP:
            rval =
                    skip_map(reader, enc, avro_schema_to_map(writers_schema));
            break;

        case AVRO_UNION:
            rval =
                    skip_union(reader, enc,
                               avro_schema_to_union(writers_schema));
            break;

        case AVRO_RECORD:
            rval =
                    skip_record(reader, enc,
                                avro_schema_to_record(writers_schema), NULL);
            break;

        case AVRO_LINK:
            rval =
                    avro_skip_data(reader,
                                   (avro_schema_to_link(writers_schema))->to);
            break;
    }

    return rval;
}
