"""Tests for stdlib csv drop-in compatibility."""
import io
import zcsv
import zcsv as csv


class TestReader:
    def test_basic_reader(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("a,b,c\n1,2,3\n4,5,6\n")
        with open(p) as f:
            r = csv.reader(f)
            rows = list(r)
        assert rows == [["a", "b", "c"], ["1", "2", "3"], ["4", "5", "6"]]

    def test_reader_delimiter(self, tmp_path):
        p = tmp_path / "test.tsv"
        p.write_text("a\tb\tc\n1\t2\t3\n")
        with open(p) as f:
            r = csv.reader(f, delimiter="\t")
            rows = list(r)
        assert rows == [["a", "b", "c"], ["1", "2", "3"]]

    def test_reader_quoted_fields(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text('name,desc\nAlice,"has, comma"\nBob,"has ""quotes"""\n')
        with open(p) as f:
            r = csv.reader(f)
            rows = list(r)
        assert rows[1] == ["Alice", "has, comma"]
        assert rows[2] == ["Bob", 'has "quotes"']

    def test_reader_line_num(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("a,b\n1,2\n3,4\n")
        with open(p) as f:
            r = csv.reader(f)
            list(r)
        assert r.line_num == 3


class TestDictReader:
    def test_basic(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("name,age\nAlice,30\nBob,25\n")
        with open(p) as f:
            dr = csv.DictReader(f)
            rows = list(dr)
        assert rows == [{"name": "Alice", "age": "30"}, {"name": "Bob", "age": "25"}]

    def test_custom_fieldnames(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("Alice,30\nBob,25\n")
        with open(p) as f:
            dr = csv.DictReader(f, fieldnames=["name", "age"])
            rows = list(dr)
        assert rows == [{"name": "Alice", "age": "30"}, {"name": "Bob", "age": "25"}]

    def test_restkey_restval(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("name,age\nAlice,30,extra\nBob\n")
        with open(p) as f:
            dr = csv.DictReader(f, restkey="_extra", restval="MISSING")
            rows = list(dr)
        assert rows[0]["_extra"] == ["extra"]
        assert rows[1]["age"] == "MISSING"


class TestWriter:
    def test_basic_writer(self):
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["a", "b", "c"])
        w.writerow(["1", "2", "3"])
        assert buf.getvalue() == "a,b,c\r\n1,2,3\r\n"

    def test_writer_quoting(self):
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["hello", "has, comma", 'has "quotes"'])
        lines = buf.getvalue()
        assert '"has, comma"' in lines
        assert '"has ""quotes"""' in lines

    def test_writer_quote_all(self):
        buf = io.StringIO()
        w = csv.writer(buf, quoting=csv.QUOTE_ALL)
        w.writerow(["a", "b"])
        assert buf.getvalue() == '"a","b"\r\n'

    def test_writerows(self):
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerows([["a", "b"], ["1", "2"]])
        assert buf.getvalue() == "a,b\r\n1,2\r\n"


class TestDictWriter:
    def test_basic(self):
        buf = io.StringIO()
        dw = csv.DictWriter(buf, fieldnames=["name", "age"])
        dw.writeheader()
        dw.writerow({"name": "Alice", "age": "30"})
        lines = buf.getvalue().split("\r\n")
        assert lines[0] == "name,age"
        assert lines[1] == "Alice,30"

    def test_extra_fields_raise(self):
        buf = io.StringIO()
        dw = csv.DictWriter(buf, fieldnames=["name"])
        try:
            dw.writerow({"name": "Alice", "extra": "bad"})
            assert False, "Should have raised ValueError"
        except ValueError:
            pass

    def test_writerows(self):
        buf = io.StringIO()
        dw = csv.DictWriter(buf, fieldnames=["name", "age"])
        dw.writeheader()
        dw.writerows([{"name": "Alice", "age": "30"}, {"name": "Bob", "age": "25"}])
        lines = buf.getvalue().strip().split("\r\n")
        assert len(lines) == 3


class TestConstants:
    def test_quote_constants(self):
        assert csv.QUOTE_MINIMAL == 0
        assert csv.QUOTE_ALL == 1
        assert csv.QUOTE_NONNUMERIC == 2
        assert csv.QUOTE_NONE == 3

    def test_field_size_limit(self):
        old = csv.field_size_limit()
        assert isinstance(old, int)
        csv.field_size_limit(999)
        assert csv.field_size_limit() == 999
        csv.field_size_limit(old)
