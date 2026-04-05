"""Tests for zcsv.write()."""
import zcsv


class TestWrite:
    def test_basic_write(self, tmp_path):
        p = tmp_path / "out.csv"
        data = [{"name": "Alice", "age": "30"}, {"name": "Bob", "age": "25"}]
        zcsv.write(str(p), data)

        content = p.read_text()
        lines = content.strip().split("\n")
        assert lines[0] == "name,age"
        assert lines[1] == "Alice,30"
        assert lines[2] == "Bob,25"

    def test_write_empty(self, tmp_path):
        p = tmp_path / "out.csv"
        zcsv.write(str(p), [])
        assert p.read_text() == ""

    def test_write_delimiter(self, tmp_path):
        p = tmp_path / "out.csv"
        data = [{"a": "1", "b": "2"}]
        zcsv.write(str(p), data, delimiter=";")
        assert "1;2" in p.read_text()

    def test_roundtrip(self, tmp_path):
        p = tmp_path / "rt.csv"
        original = [
            {"name": "Alice", "age": "30", "city": "NYC"},
            {"name": "Bob", "age": "25", "city": "LA"},
        ]
        zcsv.write(str(p), original, safe=False)
        result = zcsv.read(str(p))
        assert result[0]["name"] == "Alice"
        assert result[1]["city"] == "LA"


class TestSecurity:
    """CSV injection protection tests."""

    def test_safe_write_equals(self, tmp_path):
        p = tmp_path / "out.csv"
        data = [{"formula": "=CMD()"}]
        zcsv.write(str(p), data, safe=True)
        content = p.read_text()
        assert "'=CMD()" in content

    def test_safe_write_plus(self, tmp_path):
        p = tmp_path / "out.csv"
        data = [{"formula": "+1+1"}]
        zcsv.write(str(p), data, safe=True)
        content = p.read_text()
        assert "'+1+1" in content

    def test_safe_write_minus(self, tmp_path):
        p = tmp_path / "out.csv"
        data = [{"formula": "-1-1"}]
        zcsv.write(str(p), data, safe=True)
        content = p.read_text()
        assert "'-1-1" in content

    def test_safe_write_at(self, tmp_path):
        p = tmp_path / "out.csv"
        data = [{"formula": "@SUM(A1)"}]
        zcsv.write(str(p), data, safe=True)
        content = p.read_text()
        assert "'@SUM(A1)" in content

    def test_safe_write_tab(self, tmp_path):
        p = tmp_path / "out.csv"
        data = [{"formula": "\tmalicious"}]
        zcsv.write(str(p), data, safe=True)
        content = p.read_text()
        assert "'\tmalicious" in content

    def test_safe_false_no_prefix(self, tmp_path):
        p = tmp_path / "out.csv"
        data = [{"formula": "=CMD()"}]
        zcsv.write(str(p), data, safe=False)
        content = p.read_text()
        assert "'=CMD()" not in content

    def test_safe_default_is_true_for_write(self, tmp_path):
        """zcsv.write() defaults to safe=True."""
        p = tmp_path / "out.csv"
        data = [{"formula": "=CMD()"}]
        zcsv.write(str(p), data)  # no explicit safe param
        content = p.read_text()
        assert "'=CMD()" in content

    def test_normal_values_unchanged(self, tmp_path):
        p = tmp_path / "out.csv"
        data = [{"val": "hello"}, {"val": "123"}, {"val": ""}]
        zcsv.write(str(p), data, safe=True)
        content = p.read_text()
        assert "'hello" not in content
        assert "hello" in content


class TestRfc4180:
    def test_strict_consistent_fields(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("a,b\n1,2\n3,4\n")
        data = zcsv.read(str(p), strict=True)
        assert len(data) == 2

    def test_strict_inconsistent_fields(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("a,b\n1,2\n3,4,5\n")
        try:
            zcsv.read(str(p), strict=True)
            assert False, "Should have raised"
        except ValueError as e:
            # simd-csv catches field count mismatch at parse level
            msg = str(e).lower()
            assert "field" in msg or "rfc" in msg
