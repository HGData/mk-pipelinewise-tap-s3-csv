r"""
CSV reading with an escape character, mirroring singer_encodings' csv reader.

MadKudu addition: some customers' files were written for Redshift's
COPY ... ESCAPE, which is not CSV quoting. A backslash makes the next
character plain text, so "Yes\, I'm ready" is one value with a comma in it,
and quote marks are ordinary characters. A table opts in with
"escape_char": "\\" in its table spec; without it the library reader is used.
"""

from __future__ import annotations

import codecs
import csv
from typing import Dict, Generator

from singer_encodings import compression
from singer_encodings.csv import SDC_EXTRA_COLUMN  # pylint:disable=no-name-in-module


def get_row_iterators(
    iterable, options: Dict = None, infer_compression: bool = True
) -> Generator:
    """
    Yields one csv.DictReader per file member (a gzip file has one member; a
    zip archive may have several) — the same contract as
    singer_encodings.csv.get_row_iterators.
    """
    options = options or {}
    if infer_compression:
        iterables = compression.infer(iterable, options.get("file_name"))
    else:
        iterables = [iterable]

    for item in iterables:
        yield get_row_iterator(item, options)


def get_row_iterator(iterable, options: Dict) -> csv.DictReader:
    """
    singer_encodings.csv.get_row_iterator with the table's escape character,
    and quote marks read as ordinary characters. The rest is the same: NULL
    bytes and "remove_character" are dropped, fields past the header go to
    _sdc_extra, and missing key_properties or date_overrides headers raise.
    """
    lines = codecs.iterdecode(
        iterable, encoding=options.get("encoding", "utf-8-sig")
    )
    remove_character = options.get("remove_character", "")
    reader = csv.DictReader(
        (line.replace("\0", "").replace(remove_character, "") for line in lines),
        restkey=SDC_EXTRA_COLUMN,
        delimiter=options.get("delimiter", ","),
        escapechar=options["escape_char"],
        quoting=csv.QUOTE_NONE,
    )
    headers = set(reader.fieldnames or [])
    for key, label in (
        ("key_properties", "required headers"),
        ("date_overrides", "date_overrides headers"),
    ):
        wanted = set(options.get(key) or [])
        if not wanted.issubset(headers):
            raise ValueError(f"CSV file missing {label}: {wanted - headers}")
    return reader
