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

from tap_s3_csv import apply_bucket_url, discover, jsonl, s3, sync


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


def _rows(stream, table_spec, s3_path):
    readers = s3.row_iterators_for_table(_FakeHandle(stream), table_spec, s3_path)
    return [dict(r) for reader in readers for r in reader]


class _SelfClosingBody:
    """Like the urllib3 body behind boto3's StreamingBody: it closes itself once all its bytes are read."""

    def __init__(self, data: bytes):
        self._buf, self._size, self.closed = io.BytesIO(data), len(data), False

    def readable(self):
        return True

    def read(self, amt=-1):
        data = self._buf.read(amt)
        self.closed = self._buf.tell() >= self._size
        return data

    def readinto(self, buffer):
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)


class TestS3BodyThatClosesItself:
    # The real S3 body closes itself at the end of its data; reading it must end the rows, not raise
    # "read of closed file" (which is what broke the first dev pulls of this change).
    def test_gzipped_csv_named_csv(self):
        assert _rows(_SelfClosingBody(_gz(b"a,b\n1,x\n").getvalue()), {}, "day.csv") == [{"a": "1", "b": "x"}]

    def test_plain_csv(self):
        assert _rows(_SelfClosingBody(b"a,b\n1,x\n2,y\n"), {}, "day.csv") == [
            {"a": "1", "b": "x"}, {"a": "2", "b": "y"}
        ]

    def test_json_lines(self):
        assert _rows(_SelfClosingBody(b'{"a": "1"}\n{"a": "2"}\n'), {"format": "jsonl"}, "d.json") == [
            {"a": "1"}, {"a": "2"}
        ]

    def test_escaped_csv(self):
        assert _rows(_SelfClosingBody(b"a,b\nx\\,y,z\n"), {"escape_char": "\\"}, "d.csv") == [{"a": "x,y", "b": "z"}]


class TestGzipFromContent:
    def test_gzipped_csv_named_csv_is_read(self):
        assert _rows(_gz(b"a,b\n1,x\n"), {}, "exports/day.csv") == [
            {"a": "1", "b": "x"}
        ]

    def test_plain_csv_is_still_read_as_text(self):
        assert _rows(io.BytesIO(b"a,b\n1,x\n"), {}, "exports/day.csv") == [
            {"a": "1", "b": "x"}
        ]

    def test_gzipped_json_lines_named_json_are_read(self):
        assert _rows(
            _gz(b'{"a": "1"}\n'), {"format": "jsonl"}, "exports/day.json"
        ) == [{"a": "1"}]

    def test_empty_file_is_not_gzip(self):
        assert _rows(io.BytesIO(b""), {}, "exports/day.csv") == []


class TestEscapeChar:
    SPEC = {"escape_char": "\\"}

    def test_escaped_delimiter_stays_in_the_value(self):
        data = b"hit,type\n[Ready # Yes\\, I'm ready] - button,feature\n"
        assert _rows(io.BytesIO(data), self.SPEC, "day.csv") == [
            {"hit": "[Ready # Yes, I'm ready] - button", "type": "feature"}
        ]

    def test_without_it_the_value_is_split(self):
        # Why the setting exists: the library reader cuts the value at the comma.
        data = b"hit,type\nYes\\, ready,feature\n"
        assert _rows(io.BytesIO(data), {}, "day.csv") == [
            {"hit": "Yes\\", "type": " ready", "_sdc_extra": ["feature"]}
        ]

    def test_quote_marks_are_ordinary_characters(self):
        data = b'hit,type\n"Payroll" page,page\n/ Payroll - "Payroll",page\n'
        assert _rows(io.BytesIO(data), self.SPEC, "day.csv") == [
            {"hit": '"Payroll" page', "type": "page"},
            {"hit": '/ Payroll - "Payroll"', "type": "page"},
        ]

    def test_gzipped_file_named_csv_with_escapes(self):
        data = b"hit,type\nYes\\, ready,feature\n"
        assert _rows(_gz(data), self.SPEC, "day.csv") == [
            {"hit": "Yes, ready", "type": "feature"}
        ]

    def test_other_delimiter(self):
        data = b"a~b\nx\\~y~z\n"
        assert _rows(io.BytesIO(data), {**self.SPEC, "delimiter": "~"}, "d.csv") == [
            {"a": "x~y", "b": "z"}
        ]

    def test_extra_fields_and_null_bytes_as_the_library_does(self):
        data = b"a,b\n1\x00,2,3\n"
        assert _rows(io.BytesIO(data), self.SPEC, "day.csv") == [
            {"a": "1", "b": "2", "_sdc_extra": ["3"]}
        ]

    def test_missing_key_properties_header_raises(self):
        with pytest.raises(ValueError, match="missing required headers"):
            _rows(io.BytesIO(b"a,b\n1,2\n"), {**self.SPEC, "key_properties": ["id"]}, "d.csv")

    def test_config_accepts_it(self):
        from tap_s3_csv.config import CONFIG_CONTRACT

        CONFIG_CONTRACT([{"table_name": "t", "search_pattern": "x", "escape_char": "\\"}])

    def test_config_rejects_more_than_one_character(self):
        from voluptuous import Invalid

        from tap_s3_csv.config import CONFIG_CONTRACT

        for bad in ("\\\\", ""):
            with pytest.raises(Invalid):
                CONFIG_CONTRACT([{"table_name": "t", "search_pattern": "x", "escape_char": bad}])


class TestKeysTheSampleMissed:
    SDC = ["_sdc_source_bucket", "_sdc_source_file", "_sdc_source_lineno"]

    def _sync(self, monkeypatch, lines, known):
        written, schemas = [], []
        payload = "\n".join(json.dumps(r) for r in lines).encode()
        monkeypatch.setattr(s3, "get_file_handle", lambda config, path: _FakeHandle(_gz(payload)))
        monkeypatch.setattr(sync, "write_record", lambda name, rec, time_extracted=None: written.append(rec))
        monkeypatch.setattr(sync, "write_schema", lambda name, schema, keys: schemas.append((name, sorted(schema["properties"]), keys)))
        stream = {
            "schema": {"type": "object", "properties": {k: {"type": ["null", "string"]} for k in known + self.SDC}},
            "metadata": [{"breadcrumb": [], "metadata": {"selected": True, "table-key-properties": ["event_key"]}}],
        }
        count = sync.sync_table_file({"bucket": "b"}, "day.json.gz", {"table_name": "events", "format": "jsonl"}, stream)
        return count, written, schemas

    def test_a_rare_key_is_kept_on_every_row_that_has_it(self, monkeypatch):
        lines = [{"event_key": "1"}, {"event_key": "2", "template": "Org chart"}, {"event_key": "3", "template": "Kanban"}]
        count, written, schemas = self._sync(monkeypatch, lines, ["event_key"])
        assert count == 3
        assert [r.get("template") for r in written] == [None, "Org chart", "Kanban"]
        # The schema is sent again once, with the new key, before the first row that has it.
        assert schemas == [("events", sorted(["event_key", "template"] + self.SDC), ["event_key"])]

    def test_no_new_key_sends_no_new_schema(self, monkeypatch):
        count, written, schemas = self._sync(monkeypatch, [{"event_key": "1", "template": "x"}], ["event_key", "template"])
        assert (count, schemas) == (1, [])
        assert written[0]["template"] == "x"

    def test_a_key_differing_only_in_case_goes_into_the_known_spelling(self, monkeypatch):
        # Two spellings of one name would be one column in Redshift and fail the load.
        lines = [{"event_key": "1", "Email": "a@x"}, {"event_key": "2", "email": "b@x", "EMAIL": "c@x"}]
        count, written, schemas = self._sync(monkeypatch, lines, ["event_key", "email"])
        assert (count, schemas) == (2, [])
        assert [{k: v for k, v in r.items() if not k.startswith("_sdc")} for r in written] == [
            {"event_key": "1", "email": "a@x"}, {"event_key": "2", "email": "b@x"}
        ]

    def test_two_new_spellings_become_one_column(self, monkeypatch):
        lines = [{"event_key": "1", "Tmpl": "a"}, {"event_key": "2", "tmpl": "b"}]
        count, written, schemas = self._sync(monkeypatch, lines, ["event_key"])
        assert [r.get("Tmpl") for r in written] == ["a", "b"]
        assert all("tmpl" not in r for r in written)
        assert schemas == [("events", sorted(["event_key", "Tmpl"] + self.SDC), ["event_key"])]


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
