"""
Tests for the MadKudu fork changes: gzip routing, the jsonl format option,
s3:// bucket URLs, the assume-role credential refresher, and sampling that
ignores start_date.
"""

import gzip
import io
import json
import re
from datetime import datetime, timedelta, timezone

import pytest
from moto import mock_aws

from tap_s3_csv import apply_bucket_url, discover, jsonl, s3


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
    def test_refresh_returns_botocore_credential_shape(self, monkeypatch):
        # A developer with AWS_PROFILE set would otherwise have botocore resolve
        # that profile instead of moto's fake credentials, and fail here.
        monkeypatch.delenv("AWS_PROFILE", raising=False)
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


class TestSamplingIgnoresStartDate:
    """Columns are learned from the newest files whatever their age; start_date
    only decides which files are synced."""

    CONFIG = {"bucket": "b", "start_date": "2026-09-24T07:35:35Z"}
    TABLE = {
        "table_name": "events",
        "search_pattern": r"\.csv$",
        "key_properties": ["event_key"],
    }

    @staticmethod
    def _files(hours):
        base = datetime(2026, 9, 20, tzinfo=timezone.utc)
        return [
            {"Key": f"event/{h:03d}.csv", "LastModified": base + timedelta(hours=h), "Size": 10}
            for h in hours
        ]

    @staticmethod
    def _fake_bucket(monkeypatch, files):
        sampled = []
        monkeypatch.setattr(s3, "list_files_in_bucket", lambda *a, **k: list(files))

        def fake_sample_file(config, table_spec, s3_path, sample_rate):
            sampled.append(s3_path)
            yield {"event_key": "1", "contact_key": "a@b.c"}

        monkeypatch.setattr(s3, "sample_file", fake_sample_file)
        return sampled

    def test_columns_come_from_files_older_than_the_start_date(self, monkeypatch):
        # Every file predates start_date: the state right after a tenant moves to this tap.
        self._fake_bucket(monkeypatch, self._files(range(3)))
        schema = s3.get_sampled_schema_for_table(dict(self.CONFIG), dict(self.TABLE))
        assert {"event_key", "contact_key"} <= set(schema["properties"])

    def test_the_newest_files_are_sampled(self, monkeypatch):
        sampled = self._fake_bucket(monkeypatch, self._files([5, 1, 7, 3, 0, 6, 2, 4]))
        s3.get_sampled_schema_for_table(dict(self.CONFIG), dict(self.TABLE))
        assert sampled == [f"event/{h:03d}.csv" for h in (3, 4, 5, 6, 7)]

    def test_sync_still_skips_files_older_than_the_start_date(self, monkeypatch):
        self._fake_bucket(monkeypatch, self._files(range(3)))
        since = datetime(2026, 9, 24, 7, 35, 35, tzinfo=timezone.utc)
        assert not list(s3.get_input_files_for_table(dict(self.CONFIG), dict(self.TABLE), since))

    def test_a_table_with_no_matching_file_still_fails(self, monkeypatch):
        self._fake_bucket(monkeypatch, [])
        with pytest.raises(Exception, match="No files found"):
            s3.get_sampled_schema_for_table(dict(self.CONFIG), dict(self.TABLE))

    def test_the_no_data_error_names_the_search_pattern(self, monkeypatch):
        monkeypatch.setattr(s3, "get_sampled_schema_for_table", lambda config, spec: {})
        with pytest.raises(ValueError, match=re.escape(self.TABLE["search_pattern"])):
            discover.discover_schema(dict(self.CONFIG), dict(self.TABLE))
