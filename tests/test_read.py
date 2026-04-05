"""Tests for zcsv.read(), read_batches()."""
import zcsv


class TestRead:
    def test_basic_read(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("name,age,active\nAlice,30,true\nBob,25,false\n")
        data = zcsv.read(str(p))
        assert len(data) == 2
        assert data[0]["name"] == "Alice"
        assert data[0]["age"] == 30
        assert data[0]["active"] is True
        assert data[1]["active"] is False

    def test_type_inference_float(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("val\n1.5\n2.7\n3.14\n")
        data = zcsv.read(str(p))
        assert isinstance(data[0]["val"], float)
        assert data[2]["val"] == 3.14

    def test_type_inference_mixed_int_float(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("val\n1\n2\n3.14\n")
        data = zcsv.read(str(p))
        # Mixed int/float should become float
        assert isinstance(data[0]["val"], float)

    def test_schema_override(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("id,name\n1,Alice\n2,Bob\n")
        data = zcsv.read(str(p), schema={"id": str, "name": str})
        assert data[0]["id"] == "1"

    def test_skip_rows(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("name,age\nAlice,30\nBob,25\nCharlie,35\n")
        data = zcsv.read(str(p), skip_rows=1)
        assert len(data) == 2
        assert data[0]["name"] == "Bob"

    def test_max_rows(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("name,age\nAlice,30\nBob,25\nCharlie,35\n")
        data = zcsv.read(str(p), max_rows=2)
        assert len(data) == 2

    def test_columns_filter(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("name,age,city\nAlice,30,NYC\nBob,25,LA\n")
        data = zcsv.read(str(p), columns=["name", "city"])
        assert list(data[0].keys()) == ["name", "city"]
        assert "age" not in data[0]

    def test_null_values(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("name,age\nAlice,30\nBob,NA\nCharlie,\n")
        data = zcsv.read(str(p))
        assert data[1]["age"] is None
        assert data[2]["age"] is None

    def test_no_header(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("Alice,30\nBob,25\n")
        data = zcsv.read(str(p), has_header=False)
        assert len(data) == 2

    def test_empty_file(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("name,age\n")
        data = zcsv.read(str(p))
        assert data == []

    def test_delimiter_autodetect_tab(self, tmp_path):
        p = tmp_path / "test.tsv"
        p.write_text("name\tage\nAlice\t30\nBob\t25\n")
        data = zcsv.read(str(p))
        assert data[0]["name"] == "Alice"
        assert data[0]["age"] == 30

    def test_delimiter_autodetect_semicolon(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("name;age\nAlice;30\nBob;25\n")
        data = zcsv.read(str(p))
        assert data[0]["name"] == "Alice"


class TestReadBatches:
    def test_basic_batches(self, tmp_path):
        p = tmp_path / "test.csv"
        lines = ["id,val\n"] + [f"{i},{i*10}\n" for i in range(50)]
        p.write_text("".join(lines))

        batches = list(zcsv.read_batches(str(p), batch_size=20))
        total = sum(len(b) for b in batches)
        assert total == 50
        assert len(batches) == 3  # 20 + 20 + 10

    def test_batches_type_inference(self, tmp_path):
        p = tmp_path / "test.csv"
        lines = ["id,val\n"] + [f"{i},{i * 1.5}\n" for i in range(10)]
        p.write_text("".join(lines))

        for batch in zcsv.read_batches(str(p), batch_size=5):
            assert isinstance(batch[0]["id"], int)
            assert isinstance(batch[0]["val"], float)
