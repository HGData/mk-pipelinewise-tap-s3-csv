"""
Syncing related functions
"""

from __future__ import annotations

import copy
import csv
import sys
from itertools import chain
from typing import Dict

from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    metadata,
    utils,
    write_bookmark,
    write_record,
    write_schema,
    write_state,
)
from tap_s3_csv import s3

LOGGER = get_logger("tap_s3_csv")


def sync_stream(
    config: Dict, state: Dict, table_spec: Dict, stream: Dict
) -> int:
    """
    Sync the stream
    :param config: Connection and stream config
    :param state: current state
    :param table_spec: table specs
    :param stream: stream
    :return: count of streamed records
    """
    table_name = table_spec["table_name"] + config.get("table_suffix", "")
    modified_since = utils.strptime_with_tz(
        get_bookmark(state, table_name, "modified_since")
        or config["start_date"]
    )

    LOGGER.info('Syncing table "%s".', table_name)
    LOGGER.info("Getting files modified since %s.", modified_since)

    s3_files = s3.get_input_files_for_table(config, table_spec, modified_since)

    records_streamed = 0

    # We sort here so that tracking the modified_since bookmark makes
    # sense. This means that we can't sync s3 buckets that are larger than
    # we can sort in memory which is suboptimal. If we could bookmark
    # based on anything else then we could just sync files as we see them.
    for s3_file in sorted(s3_files, key=lambda item: item["last_modified"]):
        records_streamed += sync_table_file(
            config, s3_file["key"], table_spec, stream
        )

        state = write_bookmark(
            state,
            table_name,
            "modified_since",
            s3_file["last_modified"].isoformat(),
        )
        write_state(state)

    LOGGER.info(
        'Wrote %s records for table "%s".', records_streamed, table_name
    )

    return records_streamed


def set_empty_values_null(input_row):
    """
    Looks for empty values in the arg and sets to None. This will cause the
    results to be treated like a null value when dumped via json.dumps. This is how
    data coming from a database looks. This is useful for targets like target-snowflake
    values can be empty i.e. null in the database rather than an empty string.
    """
    ret = copy.deepcopy(input_row)
    # Handle dictionaries, lists & tuples. Scrub all values
    if isinstance(input_row, dict):
        for dict_key, dict_value in ret.items():
            ret[dict_key] = set_empty_values_null(dict_value)
    if isinstance(input_row, (list, tuple)):
        for dict_key, dict_value in enumerate(ret):
            ret[dict_key] = set_empty_values_null(dict_value)
    # If value is empty or all spaces convert to None
    if input_row == "" or str(input_row).isspace():
        ret = None
    # Finished scrubbing
    return ret


def add_new_keys(table_name: str, stream: Dict, rec: Dict, s3_path: str) -> None:
    """
    MadKudu addition: the schema was learned from a sample of rows, so a JSON
    key that only a few rows carry can be missing from it, and the Transformer
    would drop it from every row. A key the schema does not have yet is added
    as text, and the schema is sent again before the row is written.

    A key that differs from a known one only in case ("Email" next to
    "email") is folded into the known spelling instead: the loader and
    Redshift fold column names to lower case, so two spellings would clash
    as one column.
    """
    properties = stream["schema"]["properties"]
    if rec.keys() <= properties.keys():
        return
    known = {key.lower(): key for key in properties}
    new_keys = []
    for key in sorted(rec.keys() - properties.keys()):
        spelling = known.get(key.lower())
        if spelling is None:
            properties[key] = {"type": ["null", "string"]}
            known[key.lower()] = key
            new_keys.append(key)
        elif rec.get(spelling) is None:
            rec[spelling] = rec.pop(key)
        else:
            rec.pop(key)
    if not new_keys:
        return
    LOGGER.info('New columns %s in "%s"; sending the schema again.', new_keys, s3_path)
    key_properties = metadata.get(
        metadata.to_map(stream["metadata"]), (), "table-key-properties"
    )
    write_schema(table_name, stream["schema"], key_properties)


def sync_table_file(
    config: Dict, s3_path: str, table_spec: Dict, stream: Dict
) -> int:
    """
    Sync a given csv found file
    :param config: tap configuration
    :param s3_path: file path given by S3
    :param table_spec: tables specs
    :param stream: Stream data
    :return: number of streamed records
    """
    LOGGER.info('Syncing file "%s".', s3_path)

    bucket = config["bucket"]
    table_name = table_spec["table_name"] + config.get("table_suffix", "")

    s3_file_handle = s3.get_file_handle(config, s3_path)
    # We observed data who's field size exceeded the default maximum of
    # 131072. We believe the primary consequence of the following setting
    # is that a malformed, wide CSV would potentially parse into a single
    # large field rather than giving this error, but we also think the
    # chances of that are very small and at any rate the source data would
    # need to be fixed. The other consequence of this could be larger
    # memory consumption but that's acceptable as well.
    csv.field_size_limit(sys.maxsize)
    # Routed through the compression layer so gzipped files sync correctly;
    # the parser (CSV or JSON-lines) comes from the table's "format" setting.
    iterators = s3.row_iterators_for_table(s3_file_handle, table_spec, s3_path)

    records_synced = 0

    # Flattened: a zip archive yields one iterator per member, and every member
    # of the file is one stream of rows as far as this loop is concerned.
    for row in chain.from_iterable(iterators):
        time_extracted = utils.now()

        custom_columns = {
            s3.SDC_SOURCE_BUCKET_COLUMN: bucket,
            s3.SDC_SOURCE_FILE_COLUMN: s3_path,
            # index zero, +1 for header row
            s3.SDC_SOURCE_LINENO_COLUMN: records_synced + 2,
        }
        if config.get("set_empty_values_null", False):
            row = set_empty_values_null(row)

        rec = {**row, **custom_columns}
        add_new_keys(table_name, stream, rec, s3_path)

        with Transformer() as transformer:
            to_write = transformer.transform(
                rec, stream["schema"], metadata.to_map(stream["metadata"])
            )

        write_record(table_name, to_write, time_extracted=time_extracted)
        records_synced += 1

    return records_synced
