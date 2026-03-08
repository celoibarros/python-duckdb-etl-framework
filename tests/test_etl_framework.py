import unittest
from unittest.mock import MagicMock, mock_open, patch

from framework.main import CloudFileProcessor, DuckDBETL


class TestDuckDBETL(unittest.TestCase):

    @patch(
        "framework.main.fsspec.open",
        new_callable=mock_open,
        read_data="""
duckdb:
  path: ':memory:'
input:
  tables:
    - name: test_table
      path: 'file:///dummy.csv'
      format: csv
transform:
  steps:
    - sql: "SELECT 1;"
output: []
""",
    )
    @patch("framework.main.CloudFileProcessor")
    @patch("framework.main.duckdb.connect")
    def test_load_config(self, mock_connect, mock_processor, mock_fs_open):
        etl = DuckDBETL("dummy.yaml")
        self.assertEqual(etl.config["duckdb"]["path"], ":memory:")
        self.assertEqual(etl.config["input"]["tables"][0]["name"], "test_table")

    def test_sql_interpolation(self):
        etl = DuckDBETL.__new__(DuckDBETL)
        etl.parameters = {"foo": "bar", "x": 123, "none": ""}
        query = (
            "SELECT * FROM table WHERE col1 = ${foo} AND col2 = ${x} AND col3 = ${none}"
        )
        interpolated = etl._interpolate_sql(query)
        self.assertIn("'bar'", interpolated)
        self.assertIn("123", interpolated)
        self.assertIn("NULL", interpolated)

    @patch("framework.main.CloudFileProcessor._init_filesystems")
    def test_get_filesystem_for_local(self, _):
        processor = CloudFileProcessor.__new__(CloudFileProcessor)
        processor.filesystems = {"file": "mock_fs"}
        fs = processor.get_filesystem("file:///any.csv")
        self.assertEqual(fs, "mock_fs")

    @patch("framework.main.duckdb.connect")
    @patch(
        "framework.main.fsspec.open",
        new_callable=mock_open,
        read_data="""
duckdb:
  path: ':memory:'
input:
  tables: []
transform:
  steps: []
output: []
""",
    )
    def test_run_success(self, mock_open_config, mock_duckdb):
        conn = MagicMock()
        mock_duckdb.return_value = conn
        etl = DuckDBETL("dummy.yaml")
        etl.load_data = MagicMock()
        etl.transform_data = MagicMock()
        etl.export_data = MagicMock()
        etl.cleanup = MagicMock()
        etl.run()
        etl.load_data.assert_called_once()
        etl.transform_data.assert_called_once()
        etl.export_data.assert_called_once()
        etl.cleanup.assert_called_once()

    @patch("framework.main.fsspec.open", new_callable=mock_open, read_data="::bad yaml")
    def test_load_config_failure(self, mock_open):
        with self.assertRaises(Exception):
            DuckDBETL("bad.yaml")


if __name__ == "__main__":
    unittest.main()
