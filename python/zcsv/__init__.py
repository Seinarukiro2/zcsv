"""
zcsv — Blazing-fast drop-in replacement for Python's csv module, powered by Rust.

Usage:
    import zcsv as csv  # drop-in replacement for stdlib csv
"""

from zcsv._core import PyReader, PyWriter, read_csv, write_csv, sniff_delimiter
from zcsv.compat import (
    QUOTE_ALL,
    QUOTE_MINIMAL,
    QUOTE_NONNUMERIC,
    QUOTE_NONE,
    Error,
    Dialect,
    excel,
    excel_tab,
    unix_dialect,
    field_size_limit,
    Sniffer,
)
from typing import Any, Iterator, Optional, Sequence

__version__ = "0.1.0"
__all__ = [
    # stdlib compat
    "reader",
    "writer",
    "DictReader",
    "DictWriter",
    "QUOTE_ALL",
    "QUOTE_MINIMAL",
    "QUOTE_NONNUMERIC",
    "QUOTE_NONE",
    "Error",
    "Dialect",
    "excel",
    "excel_tab",
    "unix_dialect",
    "field_size_limit",
    "Sniffer",
    # zcsv extensions
    "read",
    "write",
    "read_batches",
]


# ─── stdlib-compatible reader() ───

def reader(
    csvfile,
    dialect: str = "excel",
    *,
    delimiter: str = ",",
    quotechar: str = '"',
    strict: bool = False,
    safe: bool = False,
    **kwargs,
) -> "ZcsvReader":
    """Drop-in replacement for csv.reader(). Returns an iterator of rows (list[str])."""
    if dialect == "excel_tab":
        delimiter = "\t"
    return ZcsvReader(csvfile, delimiter=delimiter, quotechar=quotechar, strict=strict, safe=safe)


class ZcsvReader:
    """Stdlib-compatible CSV reader wrapping Rust PyReader."""

    def __init__(self, csvfile, delimiter=",", quotechar='"', strict=False, safe=False):
        self._reader = PyReader(csvfile, delimiter=delimiter, quotechar=quotechar, strict=strict, safe=safe)
        self._line_num = 0

    def __iter__(self):
        return self

    def __next__(self) -> list:
        row = self._reader.__next__()
        if row is None:
            raise StopIteration
        self._line_num += 1
        return list(row)

    @property
    def line_num(self) -> int:
        return self._reader.line_num


# ─── stdlib-compatible DictReader ───
# Uses fetch_many() for batched retrieval — one Rust→Python call per batch
# instead of per row. Interning fieldnames avoids repeated str alloc.

_DICTREADER_BATCH = 4096

class DictReader:
    """Drop-in replacement for csv.DictReader()."""

    def __init__(
        self,
        f,
        fieldnames: Optional[Sequence[str]] = None,
        restkey: Optional[str] = None,
        restval: Optional[str] = None,
        dialect: str = "excel",
        *,
        delimiter: str = ",",
        quotechar: str = '"',
        strict: bool = False,
        **kwargs,
    ):
        if dialect == "excel_tab":
            delimiter = "\t"
        self._rust_reader = PyReader(f, delimiter=delimiter, quotechar=quotechar, strict=strict)
        self._fieldnames = list(fieldnames) if fieldnames else None
        self.restkey = restkey
        self.restval = restval
        self.line_num = 0
        # Batched row buffer
        self._buffer: list = []
        self._buf_idx = 0

    @property
    def fieldnames(self):
        if self._fieldnames is None:
            # Read header from first row
            self._ensure_buffer()
            if self._buffer:
                self._fieldnames = list(self._buffer[0])
                self._buf_idx = 1
            else:
                self._fieldnames = []
        return self._fieldnames

    @fieldnames.setter
    def fieldnames(self, value):
        self._fieldnames = value

    def _ensure_buffer(self):
        """Fetch next batch from Rust reader if buffer is exhausted."""
        if self._buf_idx >= len(self._buffer):
            self._buffer = self._rust_reader.fetch_many(_DICTREADER_BATCH)
            self._buf_idx = 0

    def __iter__(self):
        return self

    def __next__(self) -> dict:
        if self._fieldnames is None:
            self.fieldnames  # trigger reading header

        self._ensure_buffer()
        if self._buf_idx >= len(self._buffer):
            raise StopIteration

        row = self._buffer[self._buf_idx]
        self._buf_idx += 1
        self.line_num += 1

        # Build dict — use interned fieldnames (Python interns short strings)
        fn = self.fieldnames
        d = {}
        for i, name in enumerate(fn):
            if i < len(row):
                d[name] = row[i]
            else:
                d[name] = self.restval
        if len(row) > len(fn):
            d[self.restkey] = list(row[len(fn):])

        return d


# ─── stdlib-compatible writer() ───

def writer(
    csvfile,
    dialect: str = "excel",
    *,
    delimiter: str = ",",
    quotechar: str = '"',
    quoting: int = QUOTE_MINIMAL,
    lineterminator: str = "\r\n",
    safe: bool = False,  # safe=False for stdlib compat
    **kwargs,
) -> PyWriter:
    """Drop-in replacement for csv.writer(). Returns a writer with writerow()/writerows()."""
    if dialect == "excel_tab":
        delimiter = "\t"
    return PyWriter(
        csvfile,
        delimiter=delimiter,
        quotechar=quotechar,
        quoting=quoting,
        lineterminator=lineterminator,
        safe=safe,
    )


# ─── stdlib-compatible DictWriter ───

class DictWriter:
    """Drop-in replacement for csv.DictWriter()."""

    def __init__(
        self,
        f,
        fieldnames: Sequence[str],
        restval: str = "",
        extrasaction: str = "raise",
        dialect: str = "excel",
        *,
        delimiter: str = ",",
        quotechar: str = '"',
        quoting: int = QUOTE_MINIMAL,
        lineterminator: str = "\r\n",
        safe: bool = False,  # safe=False for stdlib compat
        **kwargs,
    ):
        self.fieldnames = list(fieldnames)
        self.restval = restval
        self.extrasaction = extrasaction
        self._writer = writer(
            f, dialect, delimiter=delimiter, quotechar=quotechar,
            quoting=quoting, lineterminator=lineterminator, safe=safe,
        )

    def writeheader(self):
        self._writer.writerow(self.fieldnames)

    def writerow(self, rowdict: dict):
        if self.extrasaction == "raise":
            wrong_fields = set(rowdict.keys()) - set(self.fieldnames)
            if wrong_fields:
                raise ValueError(f"dict contains fields not in fieldnames: {', '.join(sorted(wrong_fields))}")
        row = [str(rowdict.get(key, self.restval)) for key in self.fieldnames]
        self._writer.writerow(row)

    def writerows(self, rowdicts):
        for rowdict in rowdicts:
            self.writerow(rowdict)


# ─── zcsv extensions ───

def read(
    path: str,
    *,
    delimiter: Optional[str] = None,
    has_header: bool = True,
    schema: Optional[dict] = None,
    skip_rows: int = 0,
    max_rows: Optional[int] = None,
    columns: Optional[list] = None,
    null_values: Optional[list] = None,
    encoding: Optional[str] = None,
    strict: bool = False,
    n_threads: Optional[int] = None,
) -> list:
    """
    Read entire CSV file into list[dict] with automatic type inference.
    For large files, use read_batches() instead.
    """
    return read_csv(
        path,
        delimiter=delimiter,
        has_header=has_header,
        schema=schema,
        skip_rows=skip_rows,
        max_rows=max_rows,
        columns=columns,
        null_values=null_values,
        encoding=encoding,
        strict=strict,
        n_threads=n_threads,
    )


def write(
    path: str,
    data: list,
    *,
    delimiter: str = ",",
    safe: bool = True,  # safe=True for zcsv.write()
    strict: bool = False,
) -> None:
    """Write list[dict] to CSV file. safe=True enables CSV injection protection."""
    write_csv(path, data, delimiter=delimiter, safe=safe, strict=strict)


def read_batches(
    path: str,
    *,
    batch_size: int = 10_000,
    delimiter: Optional[str] = None,
    has_header: bool = True,
    null_values: Optional[list] = None,
    encoding: Optional[str] = None,
    strict: bool = False,
) -> Iterator[list]:
    """
    Read CSV in batches. Yields list[dict] of batch_size rows each.
    Memory-efficient for large files.
    """
    offset = 0
    while True:
        batch = read_csv(
            path,
            delimiter=delimiter,
            has_header=has_header,
            skip_rows=offset,
            max_rows=batch_size,
            null_values=null_values,
            encoding=encoding,
            strict=strict,
        )
        if not batch:
            break
        yield batch
        offset += len(batch)
