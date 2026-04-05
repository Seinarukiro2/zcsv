"""Tests for delimiter and encoding autodetection."""
import zcsv


class TestDelimiterDetection:
    def test_comma(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("a,b,c\n1,2,3\n4,5,6\n")
        data = zcsv.read(str(p))
        assert list(data[0].keys()) == ["a", "b", "c"]

    def test_tab(self, tmp_path):
        p = tmp_path / "test.tsv"
        p.write_text("a\tb\tc\n1\t2\t3\n")
        data = zcsv.read(str(p))
        assert list(data[0].keys()) == ["a", "b", "c"]

    def test_semicolon(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("a;b;c\n1;2;3\n")
        data = zcsv.read(str(p))
        assert list(data[0].keys()) == ["a", "b", "c"]

    def test_pipe(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("a|b|c\n1|2|3\n4|5|6\n")
        data = zcsv.read(str(p))
        assert list(data[0].keys()) == ["a", "b", "c"]

    def test_explicit_delimiter_overrides(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("a;b;c\n1;2;3\n")
        # Force comma even though content is semicolon
        data = zcsv.read(str(p), delimiter=",")
        assert len(data[0]) == 1  # treated as single column


class TestEncodingDetection:
    def test_utf8(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("name\nКирилл\nМарія\n", encoding="utf-8")
        data = zcsv.read(str(p))
        assert data[0]["name"] == "Кирилл"

    def test_utf8_bom(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_bytes(b"\xef\xbb\xbfname,age\nAlice,30\n")
        data = zcsv.read(str(p))
        assert data[0]["name"] == "Alice"

    def test_latin1(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_bytes(b"name,city\nAlice,Z\xfcrich\n")
        data = zcsv.read(str(p))
        assert "rich" in data[0]["city"]  # Zürich

    def test_explicit_encoding(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_bytes(b"name\nAlice\n")
        data = zcsv.read(str(p), encoding="utf-8")
        assert data[0]["name"] == "Alice"


class TestSniffer:
    def test_sniff_semicolon(self):
        s = zcsv.Sniffer()
        dialect = s.sniff("a;b;c\n1;2;3\n")
        assert dialect.delimiter == ";"

    def test_sniff_comma(self):
        s = zcsv.Sniffer()
        dialect = s.sniff("a,b,c\n1,2,3\n")
        assert dialect.delimiter == ","

    def test_has_header_true(self):
        s = zcsv.Sniffer()
        assert s.has_header("name,age,salary\nAlice,30,50000\nBob,25,60000\n")

    def test_has_header_false(self):
        s = zcsv.Sniffer()
        assert not s.has_header("1,2,3\n4,5,6\n7,8,9\n")
