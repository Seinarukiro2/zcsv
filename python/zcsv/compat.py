"""
Stdlib csv compatibility constants and classes.
"""

# Quoting constants (same values as stdlib csv)
QUOTE_MINIMAL = 0
QUOTE_ALL = 1
QUOTE_NONNUMERIC = 2
QUOTE_NONE = 3


class Error(Exception):
    """zcsv exception (stdlib csv.Error compat)."""
    pass


class Dialect:
    """Describe a CSV dialect (stdlib compat)."""

    _name = ""
    delimiter = ","
    quotechar = '"'
    escapechar = None
    doublequote = True
    skipinitialspace = False
    lineterminator = "\r\n"
    quoting = QUOTE_MINIMAL
    strict = False


class excel(Dialect):
    """Describe the usual properties of Excel-generated CSV files."""
    _name = "excel"
    delimiter = ","
    quotechar = '"'
    doublequote = True
    skipinitialspace = False
    lineterminator = "\r\n"
    quoting = QUOTE_MINIMAL


class excel_tab(excel):
    """Describe the usual properties of Excel-generated TAB-delimited files."""
    _name = "excel-tab"
    delimiter = "\t"


class unix_dialect(Dialect):
    """Describe the usual properties of Unix-generated CSV files."""
    _name = "unix"
    delimiter = ","
    quotechar = '"'
    doublequote = True
    skipinitialspace = False
    lineterminator = "\n"
    quoting = QUOTE_ALL


# Field size limit (stdlib compat)
_field_size_limit = 131072  # 128 KB default


def field_size_limit(new_limit=None):
    """Get/set the current field size limit."""
    global _field_size_limit
    old = _field_size_limit
    if new_limit is not None:
        _field_size_limit = new_limit
    return old


class Sniffer:
    """Sniff the dialect of a CSV file. Uses Rust-powered detection."""

    def sniff(self, sample: str, delimiters: str = None) -> type:
        """Analyze the sample text and return a Dialect subclass."""
        from zcsv._core import sniff_delimiter as _sniff
        import tempfile, os

        # Write sample to temp file for Rust sniffer
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(sample)
            tmp_path = f.name

        try:
            detected = _sniff(tmp_path)
        finally:
            os.unlink(tmp_path)

        # Build dialect class
        class SniffedDialect(Dialect):
            delimiter = detected
            quotechar = '"'
            doublequote = True
            skipinitialspace = False
            lineterminator = "\r\n"
            quoting = QUOTE_MINIMAL

        return SniffedDialect

    def has_header(self, sample: str) -> bool:
        """Guess whether a CSV has a header row."""
        lines = sample.strip().split("\n")
        if len(lines) < 2:
            return False

        # Heuristic: if first row has different types than second row, it's a header
        first = lines[0].split(",")
        second = lines[1].split(",")

        if len(first) != len(second):
            return False

        type_diffs = 0
        for a, b in zip(first, second):
            a_numeric = _is_numeric(a.strip())
            b_numeric = _is_numeric(b.strip())
            if a_numeric != b_numeric:
                type_diffs += 1

        return type_diffs > len(first) / 2


def _is_numeric(s: str) -> bool:
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False
