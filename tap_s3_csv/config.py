"""
Tap configuration related stuff
"""

from __future__ import annotations

from voluptuous import All, Any, Length, Optional, Required, Schema

CONFIG_CONTRACT = Schema(
    [
        {
            Required("table_name"): str,
            Required("search_pattern"): str,
            Optional("key_properties"): [str],
            Optional("search_prefix"): str,
            # MadKudu addition: how the file content is parsed. CSV is the
            # default; "jsonl" reads one JSON object per line.
            Optional("format"): Any("csv", "jsonl"),
            Optional("date_overrides"): [str],
            Optional("string_overrides"): [str],
            Optional("datatype_overrides"): object,
            Optional("guess_types"): bool,
            Optional("delimiter"): str,
            # MadKudu addition: for files written for Redshift's COPY ...
            # ESCAPE. The character makes the next one plain text, and quote
            # marks are ordinary characters (see escaped_csv.py). Exactly one
            # character, as the csv reader needs: a doubled backslash in the
            # connector's JSON is caught here, not halfway through a pull.
            Optional("escape_char"): All(str, Length(min=1, max=1)),
            Optional("table_suffix"): str,
            Optional("remove_character"): str,
            Optional("s3_proxies"): object,
            Optional("encoding"): str,
            Optional("set_empty_values_null"): bool,
        }
    ]
)
