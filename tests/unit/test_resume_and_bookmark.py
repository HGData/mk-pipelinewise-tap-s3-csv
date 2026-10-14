"""
A broken S3 read is resumed from the byte reached, and the bookmark never skips
a file that shares the last finished file's LastModified second (RGI-3174).
One test per case in the ticket's table.
"""

import gzip
import io
from datetime import datetime, timezone

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws
from urllib3.exceptions import ProtocolError

from tap_s3_csv import s3, sync

CSV = b"event_key,name\n" + b"".join(b"%d,row number %d\n" % (i, i) for i in range(2000))
ROWS = [{"event_key": str(i), "name": f"row number {i}"} for i in range(2000)]


class _Connection:
    """One open S3 read: it serves bytes from a position and breaks at the file's next break point."""

    def __init__(self, file, position):
        self.file, self.position = file, position

    def read(self, amt=-1):
        if self.file.error_at is not None and self.position >= self.file.error_at:
            raise ValueError("not a connection problem")
        if self.file.breaks and self.position >= self.file.breaks[0]:
            self.file.breaks.pop(0)
            raise ProtocolError("Connection broken: BrokenPipeError(32, 'Broken pipe')")
        end = len(self.file.data) if amt is None or amt < 0 else self.position + amt
        if self.file.breaks:
            end = min(end, self.file.breaks[0])
        chunk = self.file.data[self.position:end]
        self.position += len(chunk)
        return chunk


class _FlakyFile:
    """What get_file_handle returns, with a connection that breaks at the given byte offsets (each once)."""

    def __init__(self, data, breaks=(), failed_reopens=0, reopen_error=None, error_at=None):
        self.data, self.breaks = data, list(breaks)
        self.failed_reopens, self.reopen_error, self.error_at = failed_reopens, reopen_error, error_at
        self.opened_at = [0]
        self._raw_stream = _Connection(self, 0)

    def reopen(self, position):
        self.opened_at.append(position)
        if self.reopen_error:
            raise self.reopen_error
        if self.failed_reopens:
            self.failed_reopens -= 1
            raise ProtocolError("Connection broken: still down")
        return _Connection(self, position)


def _rows(handle, table_spec=None, path="day.csv"):
    readers = s3.row_iterators_for_table(handle, table_spec or {}, path)
    return [dict(r) for reader in readers for r in reader]


@pytest.fixture(autouse=True)
def no_waiting(monkeypatch):
    waits = []
    monkeypatch.setattr(s3.time, "sleep", waits.append)
    return waits


class TestBrokenReads:
    def test_1_a_normal_read_is_unchanged(self):
        handle = _FlakyFile(CSV)
        assert _rows(handle) == ROWS
        assert handle.opened_at == [0]

    def test_2_one_broken_read_resumes_from_the_byte_reached(self, no_waiting):
        middle = len(CSV) // 2 + 3  # inside a line on purpose
        handle = _FlakyFile(CSV, breaks=[middle])
        assert _rows(handle) == ROWS
        assert handle.opened_at == [0, middle]
        assert no_waiting == [1]

    def test_3_it_keeps_resuming_when_the_read_breaks_again(self, no_waiting):
        handle = _FlakyFile(CSV, breaks=[1000, 9000, 20000], failed_reopens=4)
        assert _rows(handle) == ROWS
        assert handle.opened_at[-3:] == [1000, 9000, 20000]
        assert no_waiting[:5] == [1, 2, 4, 8, 16]  # the wait doubles on tries in a row

    def test_4_it_gives_up_after_five_tries_in_a_row(self):
        handle = _FlakyFile(CSV, breaks=[1000], failed_reopens=100)
        with pytest.raises(ProtocolError):
            _rows(handle)
        assert len(handle.opened_at) == 1 + s3.READ_RETRIES

    def test_5_a_file_replaced_meanwhile_is_not_joined_from_two_versions(self):
        changed = ClientError({"Error": {"Code": "PreconditionFailed", "Message": "At least one of the pre-conditions you specified did not hold"}}, "GetObject")
        handle = _FlakyFile(CSV, breaks=[1000], reopen_error=changed)
        with pytest.raises(ClientError, match="PreconditionFailed"):
            _rows(handle)
        assert handle.opened_at == [0, 1000]

    def test_6_a_gzipped_file_resumes_mid_stream(self):
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(CSV)
        data = buf.getvalue()
        handle = _FlakyFile(data, breaks=[len(data) // 3, 2 * len(data) // 3])
        assert _rows(handle, path="day.csv.gz") == ROWS

    def test_6b_json_lines_resume_too(self):
        lines = b"".join(b'{"event_key": "%d"}\n' % i for i in range(3000))
        handle = _FlakyFile(lines, breaks=[len(lines) // 2 + 5])
        assert _rows(handle, {"format": "jsonl"}, "day.json") == [{"event_key": str(i)} for i in range(3000)]

    def test_7_other_errors_are_not_retried(self):
        handle = _FlakyFile(CSV, error_at=500)
        with pytest.raises(ValueError, match="not a connection problem"):
            _rows(handle)
        assert handle.opened_at == [0]


class TestS3FileHandle:
    BUCKET, KEY = "tenant-bucket", "event/day.csv"

    @pytest.fixture
    def bucket(self, monkeypatch):
        for name, value in {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test", "AWS_DEFAULT_REGION": "us-east-1"}.items():
            monkeypatch.setenv(name, value)
        monkeypatch.delenv("AWS_PROFILE", raising=False)
        monkeypatch.setattr(boto3, "DEFAULT_SESSION", None)
        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket=self.BUCKET)
            client.put_object(Bucket=self.BUCKET, Key=self.KEY, Body=CSV)
            yield client

    def test_reopen_reads_the_rest_of_the_same_file(self, bucket):
        handle = s3.get_file_handle({"bucket": self.BUCKET}, self.KEY)
        assert handle.reopen(100).read() == CSV[100:]

    def test_reopen_of_a_replaced_file_fails(self, bucket):
        handle = s3.get_file_handle({"bucket": self.BUCKET}, self.KEY)
        bucket.put_object(Bucket=self.BUCKET, Key=self.KEY, Body=CSV + b"9999,new row\n")
        with pytest.raises(ClientError) as err:
            handle.reopen(100)
        assert err.value.response["Error"]["Code"] in ("PreconditionFailed", "412")

    def test_reopen_at_the_very_end_reads_nothing(self, bucket):
        handle = s3.get_file_handle({"bucket": self.BUCKET}, self.KEY)
        assert handle.reopen(len(CSV)).read() == b""

    def test_rows_come_through_the_real_handle(self, bucket):
        assert _rows(s3.get_file_handle({"bucket": self.BUCKET}, self.KEY)) == ROWS


class TestBookmark:
    """Files A..E: A and B at 10:00:00, C, D and E all at 10:57:51 (one export uploaded in the same second)."""

    T1 = datetime(2026, 10, 13, 10, 0, 0, tzinfo=timezone.utc)
    T2 = datetime(2026, 10, 13, 10, 57, 51, tzinfo=timezone.utc)
    CONFIG = {"bucket": "b", "start_date": "2026-10-06T00:00:00Z"}
    TABLE = {"table_name": "events", "search_pattern": r"\.json\.gz$"}

    def _files(self):
        return [
            {"Key": "e/part_e.json.gz", "LastModified": self.T2, "Size": 1},
            {"Key": "e/part_a.json.gz", "LastModified": self.T1, "Size": 1},
            {"Key": "e/part_d.json.gz", "LastModified": self.T2, "Size": 1},
            {"Key": "e/part_b.json.gz", "LastModified": self.T1, "Size": 1},
            {"Key": "e/part_c.json.gz", "LastModified": self.T2, "Size": 1},
        ]

    def _run(self, monkeypatch, state, fail_on=None):
        """One sync run; returns the keys it read and the state it saved last."""
        read, saved = [], [state]
        monkeypatch.setattr(s3, "list_files_in_bucket", lambda *a, **k: self._files())
        monkeypatch.setattr(sync, "write_state", lambda st: saved.append(dict(st, bookmarks={k: dict(v) for k, v in st.get("bookmarks", {}).items()})))

        def fake_sync_table_file(config, key, table_spec, stream):
            read.append(key.split("_")[-1][0])
            if key.endswith(f"part_{fail_on}.json.gz"):
                raise ProtocolError("Connection broken: BrokenPipeError(32, 'Broken pipe')")
            return 1

        monkeypatch.setattr(sync, "sync_table_file", fake_sync_table_file)
        try:
            sync.sync_stream(dict(self.CONFIG), state, dict(self.TABLE), {})
        except ProtocolError:
            pass
        return read, saved[-1]

    def test_files_are_read_in_time_then_name_order(self, monkeypatch):
        read, state = self._run(monkeypatch, {})
        assert read == ["a", "b", "c", "d", "e"]
        assert state["bookmarks"]["events"] == {"modified_since": self.T2.isoformat(), "modified_since_key": "e/part_e.json.gz"}

    def test_8_a_file_that_failed_is_read_again_next_run(self, monkeypatch):
        read, state = self._run(monkeypatch, {}, fail_on="c")
        assert read == ["a", "b", "c"]
        assert state["bookmarks"]["events"] == {"modified_since": self.T1.isoformat(), "modified_since_key": "e/part_b.json.gz"}
        read, _ = self._run(monkeypatch, state)
        assert read == ["c", "d", "e"]

    def test_9_files_in_the_same_second_are_not_skipped(self, monkeypatch):
        # 13 Oct: the run broke inside a file that shared its second with the last finished one.
        read, state = self._run(monkeypatch, {}, fail_on="d")
        assert read == ["a", "b", "c", "d"]
        assert state["bookmarks"]["events"]["modified_since_key"] == "e/part_c.json.gz"
        read, _ = self._run(monkeypatch, state)
        assert read == ["d", "e"]

    def test_10_no_new_files_reads_nothing(self, monkeypatch):
        state = {"bookmarks": {"events": {"modified_since": self.T2.isoformat(), "modified_since_key": "e/part_e.json.gz"}}}
        read, _ = self._run(monkeypatch, state)
        assert read == []

    def test_11_an_old_bookmark_without_a_key_behaves_as_before(self, monkeypatch):
        state = {"bookmarks": {"events": {"modified_since": self.T1.isoformat()}}}
        read, state = self._run(monkeypatch, state)
        assert read == ["c", "d", "e"]  # the T1 second is skipped whole, as before
        assert state["bookmarks"]["events"]["modified_since_key"] == "e/part_e.json.gz"
