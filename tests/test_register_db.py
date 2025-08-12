
import unittest
import yaml
import os
import shutil
from pathlib import Path
from sqlalchemy import create_engine, text
from utils.register_db import register_database

class TestRegisterDB(unittest.TestCase):
    def setUp(self):
        self.test_dir = Path("test_temp_dir")
        self.test_dir.mkdir()
        self.tools_yaml_path = self.test_dir / "tools.yaml"
        self.db_key = "test_sqlite"
        self.db_url = f"sqlite:///{self.test_dir / 'test.db'}"
        self.engine = create_engine(self.db_url)
        with self.engine.connect() as connection:
            connection.execute(text("CREATE TABLE test_table (id INTEGER, name VARCHAR)"))

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_register_sqlite_database(self):
        tables = register_database(str(self.tools_yaml_path), self.db_key, self.db_url)
        self.assertEqual(tables, ["test_table"])

        with open(self.tools_yaml_path, 'r') as f:
            config = yaml.safe_load(f)

        self.assertIn("sources", config)
        self.assertIn(self.db_key, config["sources"])
        self.assertEqual(config["sources"][self.db_key]["kind"], "sqlite")
        self.assertEqual(config["sources"][self.db_key]["database"], str(self.test_dir / 'test.db'))

        self.assertIn("tools", config)
        self.assertIn(f"{self.db_key}_list_tables", config["tools"])
        self.assertIn(f"{self.db_key}_describe_table", config["tools"])
        self.assertIn(f"{self.db_key}_execute_query", config["tools"])

        self.assertIn("toolsets", config)
        self.assertIn(f"{self.db_key}_toolset", config["toolsets"])
        self.assertEqual(len(config["toolsets"][f"{self.db_key}_toolset"]), 3)

if __name__ == '__main__':
    unittest.main()
