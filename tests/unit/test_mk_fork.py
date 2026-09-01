"""
Tests for the MadKudu fork changes: gzip routing, the jsonl format option,
s3:// bucket URLs, and the assume-role credential refresher.
"""

import gzip
import io
import json

import pytest
from moto import mock_aws

from tap_s3_csv import apply_bucket_url, jsonl, s3


def _gz(data: bytes) -> io.BytesIO:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as handle:
        handle.write(data)
    buf.seek(0)
    return buf


class TestJsonlReader:
    def test_reads_gzipped_json_lines(self):
        rows = [{"a": "1", "b": "x"}, {"a": "2", "b": "y"}]
        payload = "\n".join(json.dumps(r) for r in rows).encode()
        readers = jsonl.get_row_iterators(
            _gz(payload), options={"file_name": "f.jsonl.gz"}
        )
        out = [row for reader in readers for row in reader]
        assert out == rows

    def test_reads_plain_json_lines(self):
        payload = b'{"a": 1}\n\n{"a": 2}\n'
        readers = jsonl.get_row_iterators(
            io.BytesIO(payload),
            options={"file_name": "f.jsonl"},
            infer_compression=False,
        )
        out = [row for reader in readers for row in reader]
        assert out == [{"a": 1}, {"a": 2}]

    def test_invalid_json_line_raises_with_line_number(self):
        payload = b'{"a": 1}\nnot json\n'
        readers = jsonl.get_row_iterators(
            io.BytesIO(payload), options={}, infer_compression=False
        )
        with pytest.raises(ValueError, match="Line 2"):
            for reader in readers:
                list(reader)

    def test_non_object_line_raises(self):
        readers = jsonl.get_row_iterators(
            io.BytesIO(b"[1, 2]\n"), options={}, infer_compression=False
        )
        with pytest.raises(ValueError, match="not an object"):
            for reader in readers:
                list(reader)


class _FakeHandle:  # what get_file_handle returns, as far as the router cares
    def __init__(self, stream):
        self._raw_stream = stream


class TestFormatRouting:
    def test_jsonl_format_uses_json_reader(self):
        handle = _FakeHandle(_gz(b'{"a": "1"}\n'))
        readers = s3.row_iterators_for_table(
            handle, {"format": "jsonl"}, "f.jsonl.gz"
        )
        assert [r for reader in readers for r in reader] == [{"a": "1"}]

    def test_default_is_csv_and_gzip_is_inferred(self):
        handle = _FakeHandle(_gz(b"a,b\n1,x\n"))
        readers = s3.row_iterators_for_table(
            handle, {"delimiter": ","}, "f.csv.gz"
        )
        assert [dict(r) for reader in readers for r in reader] == [
            {"a": "1", "b": "x"}
        ]


class TestBucketUrl:
    def test_plain_bucket_name_is_untouched(self):
        config = {"bucket": "my-bucket", "tables": [{"search_prefix": "x"}]}
        apply_bucket_url(config)
        assert config["bucket"] == "my-bucket"
        assert config["tables"][0]["search_prefix"] == "x"

    def test_url_without_folder(self):
        config = {"bucket": "s3://their-bucket", "tables": [{}]}
        apply_bucket_url(config)
        assert config["bucket"] == "their-bucket"
        assert "search_prefix" not in config["tables"][0]

    def test_url_folder_prepends_to_search_prefix(self):
        config = {
            "bucket": "s3://their-bucket/exports/daily/",
            "tables": [{"search_prefix": "events"}, {}],
        }
        apply_bucket_url(config)
        assert config["bucket"] == "their-bucket"
        assert config["tables"][0]["search_prefix"] == "exports/daily/events"
        assert config["tables"][1]["search_prefix"] == "exports/daily"

    def test_url_with_no_bucket_name_raises(self):
        with pytest.raises(ValueError):
            apply_bucket_url({"bucket": "s3:///nothing", "tables": []})


class TestAssumeRoleRefresher:
    @mock_aws
    def test_refresh_returns_botocore_credential_shape(self):
        refresh = s3._assume_role_refresher(  # pylint:disable=protected-access
            {
                "role_arn": "arn:aws:iam::123456789012:role/customer",
                "external_id": "mk-ext",
                "aws_region": "us-west-2",
            }
        )
        creds = refresh()
        assert set(creds) == {
            "access_key",
            "secret_key",
            "token",
            "expiry_time",
        }
        assert creds["access_key"]
        # expiry must parse as ISO so botocore can schedule the next refresh
        assert "T" in creds["expiry_time"]
