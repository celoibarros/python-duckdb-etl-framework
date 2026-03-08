# etl_framework.py (with lazy loading and memory usage logging)
import importlib
import logging
import os
import re
import shutil
import tempfile
import time
from argparse import ArgumentParser

import duckdb
import fsspec
import psutil
import yaml
from adlfs import AzureBlobFileSystem

from framework.azure_credential import AzureCredential, TokenCredential
from azure.keyvault.secrets import SecretClient
from database.connection_string import valid_connection_string


def configure_logger(log_level_str="WARNING"):
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler()],
    )
    return logging.getLogger(__name__)


def log_memory_usage(label=""):
    process = psutil.Process(os.getpid())
    mem_mb = process.memory_info().rss / (1024 * 1024)
    logger.warning(f"[MEMORY] {label} - RSS Memory Usage: {mem_mb:.2f} MB")


class CloudFileProcessor:
    def __init__(self, config):
        self.config = config
        logger.warning("Initializing CloudFileProcessor")
        self.filesystems = self._init_filesystems()

    def _init_filesystems(self):
        fs_map = {}
        azure_list = self.config["duckdb"].get("azure", [])
        if isinstance(azure_list, dict):
            azure_list = [azure_list]

        for azure_cfg in azure_list:
            name = azure_cfg["account_name"]
            logger.warning(f"Setting up AzureBlobFileSystem for account: {name}")

            if azure_cfg.get("managed_identity_client_id"):
                umi = azure_cfg["managed_identity_client_id"]
                credential = AzureCredential(
                    tenant_id=azure_cfg["tenant_id"], managed_identity_id=umi
                ).get_credential()
                fs = AzureBlobFileSystem(account_name=name, credential=credential)
            elif azure_cfg.get("access_token"):
                token = azure_cfg["access_token"]
                token_credential = TokenCredential(token)
                credential = AzureCredential(
                    tenant_id=azure_cfg["tenant_id"], credentials=(token_credential,)
                ).get_credential()
                fs = AzureBlobFileSystem(account_name=name, credential=credential)
            else:
                fs = AzureBlobFileSystem(
                    account_name=name,
                    tenant_id=azure_cfg.get("tenant_id"),
                    account_key=azure_cfg.get("account_key"),
                    client_id=azure_cfg.get("client_id"),
                    client_secret=azure_cfg.get("client_secret"),
                )

            fs_map[f"abfs_{name}"] = fs

        fs_map["file"] = fsspec.filesystem("file")
        return fs_map

    def get_filesystem(self, path, account=None):
        if path.startswith("abfs://") and account:
            return self.filesystems.get(f"abfs_{account}", self.filesystems["file"])
        from urllib.parse import urlparse

        scheme = urlparse(path).scheme.split("+")[0]
        return self.filesystems.get(scheme, self.filesystems["file"])

    def download_to_local(self, path: str, account: str) -> tuple[str, str]:
        fs = self.filesystems.get(f"abfs_{account}")
        if not fs:
            raise ValueError(f"No ABFS filesystem found for account '{account}'")

        local_dir = tempfile.mkdtemp(prefix="abfs_download_")
        remote_files = fs.glob(path)
        if not remote_files:
            raise FileNotFoundError(f"No files found for path {path}")

        for remote_file in remote_files:
            # remove "abfs://" to get relative path
            rel_path = remote_file.replace("abfs://", "")
            local_file = os.path.join(local_dir, rel_path)
            os.makedirs(os.path.dirname(local_file), exist_ok=True)

            logger.warning(f"Downloading file {remote_file} to {local_file}")
            with fs.open(remote_file, "rb") as fsrc, open(local_file, "wb") as fdst:
                shutil.copyfileobj(fsrc, fdst)

        # 🔑 Build the local pattern **inside the temp folder**
        rel_pattern = path.replace("abfs://", "")
        local_pattern = os.path.join(local_dir, rel_pattern)

        # Normalize slashes for DuckDB
        local_pattern = local_dir.replace("\\", "/") + f"/{rel_pattern}"

        logger.warning(
            f"Downloaded {len(remote_files)} files to {local_dir} "
            f"(local pattern for DuckDB: {local_pattern})"
        )

        return local_pattern, local_dir


class DuckDBETL:
    def __init__(self, config_path, parameters=None, file_processor=None):
        logger.warning("Initializing DuckDBETL")
        self.parameters = parameters or {}
        self.config = self.load_config(config_path)
        self.conn = self.setup_connection()
        self.file_processor = file_processor or CloudFileProcessor(self.config)
        self.temp_tables = []
        self.temp_local_paths = []

    def load_config(self, config_path):
        logger.warning(f"Loading config from {config_path}")
        with fsspec.open(config_path, "r") as f:
            raw = f.read()

        self.config_raw = yaml.safe_load(raw)
        self.kv_vault_url = self.config_raw.get("duckdb", {}).get("vault")
        self.kv_clients = {}

        def get_kv_secret(secret_name):
            if not self.kv_vault_url:
                raise ValueError(
                    "Key Vault URL not defined in config under duckdb.vault"
                )
            if self.kv_vault_url not in self.kv_clients:
                azure_credential = AzureCredential().get_credential()
                self.kv_clients[self.kv_vault_url] = SecretClient(
                    vault_url=self.kv_vault_url, credential=azure_credential
                )
            return self.kv_clients[self.kv_vault_url].get_secret(secret_name).value

        def replace_placeholder(match):
            key = match.group(1)
            # Key Vault secrets: resolve immediately
            if key.startswith("kv:"):
                secret_name = key.split("kv:")[1]
                try:
                    return get_kv_secret(secret_name)
                except Exception as e:
                    logger.warning(f"Failed to get secret '{secret_name}' from Key Vault: {e}")
                    return ""

            # CLI / runtime parameters that were passed when instantiating DuckDBETL
            if key in self.parameters:
                val = self.parameters[key]
                # if None or empty, keep original placeholder so later logic can decide
                return str(val) if val is not None else match.group(0)

            # Environment variables
            if key in os.environ:
                return os.environ[key]

            # Unknown placeholder: leave it intact for runtime interpolation
            return match.group(0)

        raw = re.sub(r"\${([^}]+)}", replace_placeholder, raw)
        return yaml.safe_load(raw)

    def setup_connection(self):
        logger.warning("Setting up DuckDB connection")
        conn = duckdb.connect(self.config["duckdb"]["path"])
        for key, value in self.config["duckdb"].get("pragmas", {}).items():
            if value is None:
                conn.execute(f"PRAGMA {key};")
            else:
                conn.execute(f'PRAGMA {key}="{value}";')
        conn.sql("select * from duckdb_settings()").show(max_rows=1000000)

        for ext in self.config["duckdb"].get("extensions", []):
            logger.warning(f"Loading extension: {ext}")
            conn.execute(f"INSTALL {ext}; LOAD {ext};")

        return conn

    def load_data(self):
        logger.warning("Setting up input tables")
        self.input_table_meta = {}
        for table in self.config["input"]["tables"]:
            self.input_table_meta[table["name"]] = table

        if not self.config["duckdb"].get("lazy_load", True):
            for table in self.config["input"]["tables"]:
                self._load_single_table(table)

    def _load_single_table(self, table):
        name = table["name"]
        prefix = "[LAZY LOAD] " if self.config["duckdb"].get("lazy_load", True) else ""
        logger.warning(f"{prefix}Loading table: {name}")
        type_ = table.get("type", "file")

        if type_ in ["abfs", "file"]:
            path = table["path"]
            account = table.get("account_name")
            use_local = table.get("local_download", False)
            format = table["format"]
            fs = self.file_processor.get_filesystem(path, account=account)
            self.conn.register_filesystem(fs)

            if use_local and type_ == "abfs":
                path, temp_dir = self.file_processor.download_to_local(path, account)
                self.temp_local_paths.append(temp_dir)
                fs = self.file_processor.get_filesystem(path, account=account)
                self.conn.register_filesystem(fs)

            self.conn.execute(
                f"""
                CREATE OR REPLACE TEMP TABLE {name} AS
                SELECT * FROM read_{format}('{path}', HIVE_PARTITIONING=true)
                """
            )

            if self.config.get("debug", False):
                logger.warning(f"Input table {name} preview")
                self.conn.sql(
                    f"""
                    SELECT * FROM {name} limit 5
                """
                ).show(max_width=250)

        elif type_ == "sql_server":
            import pandas as pd
            import pyodbc

            source = table["source"]
            query = self._interpolate_sql(table["query"])
            conn_str = next(
                s["connection_string"]
                for s in self.config["duckdb"]["sql_server"]
                if s["name"] == source
            )
            conn_data = valid_connection_string(conn_str)

            if table.get("pushdown") and "pushdown_filter_from" in table:
                for dep in table["pushdown_filter_from"]:
                    dep_table = dep["table"]
                    dep_column = dep["column"]

                    # Fetch distinct values from dependency
                    dep_query = f"SELECT DISTINCT {dep_column} FROM {dep_table}"
                    values_df = self.conn.execute(dep_query).fetchdf()
                    values = values_df[dep_column].dropna().unique().tolist()

                    if not values:
                        continue

                    values_str = ", ".join(f"'{v}'" for v in values)
                    filter_clause = f"{dep_column} IN ({values_str})"

                    # Modify the SQL query in place
                    original_query = table["query"]
                    if "WHERE" in original_query.upper():
                        updated_query = re.sub(
                            r"(WHERE\s+)",
                            r"\1" + filter_clause + " AND ",
                            original_query,
                            flags=re.IGNORECASE,
                        )
                    else:
                        updated_query = (
                            original_query.strip().rstrip(";")
                            + f" WHERE {filter_clause}"
                        )

                    query = updated_query
                    logger.warning(
                        f"[PUSHDOWN] Applied filter from {dep_table} into {name}"
                    )

            sql_conn = pyodbc.connect(
                conn_data["connection_string"],
                attrs_before=conn_data.get("connect_args", {}).get("attrs_before", {}),
            )

            if self.config["duckdb"].get("debug", True):
                logger.warning(f"Executing query for {name}: {query}...")

            chunks = pd.read_sql(
                query, sql_conn, chunksize=table.get("chunksize", 100000)
            )
            for i, chunk in enumerate(chunks):
                table_name = f"{name}_chunk_{i}"
                logger.warning(f"Loading chunk {i + 1} for table {name}")
                self.conn.register(table_name, chunk)
                self.conn.execute(
                    f"create temp table if not exists {name} as SELECT * FROM {table_name} where 1 != 1"
                )
                self.conn.execute(f"insert into {name} SELECT * FROM {table_name}")
                self.conn.unregister(table_name)
            sql_conn.close()

        self.temp_tables.append(name)
        log_memory_usage(f"After loading table '{name}'")

    def _ensure_lazy_table_loaded(self, sql_query: str):
        for name in self.input_table_meta:
            pattern = rf"(?:^|[^a-zA-Z0-9_]){re.escape(name)}(?:[^a-zA-Z0-9_]|$)"
            if (
                re.search(pattern, sql_query, re.IGNORECASE)
                and name not in self.temp_tables
            ):
                self._load_single_table(self.input_table_meta[name])

    def clean_unused_tables(self):
        remaining_queries = [
            step.get("sql") or step.get("query", "")
            for step in self.config["transform"]["steps"]
            if step["type"] in ["sql", "sql_file"]
        ]
        full_script = " ".join(remaining_queries).lower()

        for table in list(self.temp_tables):
            if (
                table in self.used_tables
                and table.lower() not in full_script
                and table not in {o["table"] for o in self.config.get("output", [])}
            ):
                try:
                    logger.warning(f"Dropping unused table: {table}")
                    self.conn.execute(f"DROP TABLE IF EXISTS {table}")
                    self.temp_tables.remove(table)
                except Exception as e:
                    logger.warning(f"Failed to drop table {table}: {e}")

    def transform_data(self):
        logger.warning("Starting transformation")
        for step in self.config["transform"]["steps"]:
            if step["type"] in ["sql", "sql_file"]:
                sql = step.get("sql") or step.get("query")
                sql = self._interpolate_sql(sql)
                self._ensure_lazy_table_loaded(sql)

                if step["type"] == "sql_file":
                    fs = self.file_processor.get_filesystem(step["path"])
                    with fs.open(step["path"], "r") as f:
                        sql = f.read()

                logging.warning(
                    f"Executing SQL step: {sql[:100]}..."
                )  # Log first 100 chars for brevity

                if "register_as_name" in step:
                    query = f"""
                        CREATE OR REPLACE TEMP TABLE {step["register_as_name"]} AS
                            SELECT * FROM ({sql.replace(";", "")})
                    """
                    self.conn.execute(query)
                else:
                    self.conn.execute(sql)

                log_memory_usage(f"After SQL step")

            elif step["type"] == "python":
                script = step["script"]
                logger.warning(f"Executing script from path {script}")
                self._execute_python_transform(script)
                log_memory_usage(f"After Python script {script}")
            elif step["type"] == "smallpond_sql":
                self._execute_smallpond_sql_step(step)
                log_memory_usage("After smallpond_sql step")

            if self.config.get("clean_unused", False):
                self.clean_unused_tables()

    def _execute_python_transform(self, script_path):
        spec = importlib.util.spec_from_file_location("transform_module", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Provide full ETL context to the transform script
        context = {
            "conn": self.conn,
            "parameters": self.parameters,
            "config": self.config,
            "logger": logger,
            "file_processor": self.file_processor
        }

        if hasattr(module, "transform"):
            module.transform(**context)
        else:
            raise AttributeError(f"Transform script {script_path} has no 'transform' function")

    def _execute_smallpond_sql_step(self, step):
        try:
            smallpond = importlib.import_module("smallpond")
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "smallpond is not installed. Install optional dependency "
                "with: pip install smallpond"
            ) from exc

        sql = self._interpolate_sql(step.get("sql") or step.get("query"))
        input_path = self._interpolate_sql(step["input_path"])
        output_path = self._interpolate_sql(step["output_path"])

        logger.warning(
            f"Executing distributed SQL with smallpond: input={input_path}, output={output_path}"
        )

        sp = smallpond.init()
        dataframe = sp.read_parquet(input_path)

        repartition = step.get("repartition")
        if repartition:
            hash_by = step.get("hash_by")
            if hash_by:
                dataframe = dataframe.repartition(repartition, hash_by=hash_by)
            else:
                dataframe = dataframe.repartition(repartition)

        result = sp.partial_sql(sql, dataframe)
        result.write_parquet(output_path)

    def _interpolate_sql(self, raw_sql):
        def replacer(match):
            key = match.group(1)
            value = None

            # 1️⃣ First priority: explicit parameters
            if key in self.parameters:
                value = self.parameters[key]

            # 2️⃣ Second: environment variable
            elif key in os.environ:
                value = os.environ[key]

            # 3️⃣ Third: DuckDB table/column lookup
            else:
                try:
                    # check if a temp table contains this column
                    tables = [t[0] for t in self.conn.execute("SHOW TABLES").fetchall()]
                    for table in tables:
                        cols = [c[0].lower() for c in self.conn.execute(f"DESCRIBE {table}").fetchall()]
                        if key.lower() in cols:
                            # select first value from that column
                            result = self.conn.execute(f"SELECT {key} FROM {table} LIMIT 1").fetchone()
                            if result:
                                value = result[0]
                                logger.warning(f"[PARAM] Using {key}={value} from table '{table}'")
                                break
                except Exception as e:
                    logger.warning(f"Failed to lookup {key} in DuckDB tables: {e}")

            # 4️⃣ Default/fallback
            if value is None or value == "":
                return "NULL"
            elif isinstance(value, str):
                safe_value = value.replace("'", "''")
                return f"'{safe_value}'"
            else:
                return str(value)

        return re.sub(r"\${(\w+)}", replacer, raw_sql)

    def export_data(self):
        for output in self.config.get("output", []):
            table, path, fmt = output["table"], output["path"], output["format"]
            partition_by = output.get("partition_by", [])
            overwrite = output.get("overwrite", True)
            account = output.get("account_name", None)
            
            fs = self.file_processor.get_filesystem(path, account=account)
            self.conn.register_filesystem(fs)
            
            logger.warning(f"Exporting table {table} to {path} as {fmt}")
            
            if self.config["duckdb"].get("debug", True):
                logger.warning(f"Output table {table} preview")
                self.conn.sql(f"""
                    SELECT * FROM {table} limit 5
                """).show(max_width=250)
                       
            logger.warning(f"Exporting table {table} to {path} as {fmt}")
            stmt = f"COPY {table} TO '{path}' (FORMAT {fmt}"
            if partition_by:
                stmt += f", PARTITION_BY ({', '.join(partition_by)})"
            if overwrite:
                stmt += ", OVERWRITE_OR_IGNORE"
            stmt += ")"
            self.conn.execute(stmt)
            log_memory_usage(f"After exporting {table}")


    def stg_cleanup(self):
        """Clean specified staging folders defined in config under duckdb.cleanup_stg_folders"""
        folders = self.config.get("cleanup_stg_folders", [])
        logger.warning(f"Cleaning up {len(folders)} staging folders")
        for folder in folders:
            fs = self.file_processor.get_filesystem(folder.get("path"), account=folder.get("account_name"))
            path = folder["path"]
            try:
                if fs.exists(path):
                    logger.warning(f"Cleaning staging folder: {path}")
                    fs.rm(path, recursive=True)
            except Exception as e:
                logger.error(f"Failed to clean staging folder {path}: {e}")

    def run(self):
        stage_times = {}
        stage_memory = {}
        total_start = time.time()
        logger.warning("🚀 Starting full ETL execution")

        def record_stage(stage_name, func):
            start = time.time()
            func()
            elapsed = time.time() - start
            stage_times[stage_name] = elapsed

            # Capture memory usage (MB)
            process = psutil.Process(os.getpid())
            mem_mb = process.memory_info().rss / (1024 * 1024)
            stage_memory[stage_name] = mem_mb
            logger.warning(f"⏱ {stage_name} took {elapsed:.2f}s — memory: {mem_mb:.2f} MB")

        try:
            record_stage("load_data", self.load_data)
            record_stage("transform_data", self.transform_data)
            record_stage("export_data", self.export_data)
            record_stage("stg_cleanup", self.stg_cleanup)
        finally:
            record_stage("cleanup", self.cleanup)

            total_elapsed = time.time() - total_start
            logger.warning("📊 ====== ETL EXECUTION SUMMARY ======")
            for stage in stage_times:
                logger.warning(
                    f"{stage:<15} | Time: {stage_times[stage]:>8.2f}s | "
                    f"Memory: {stage_memory[stage]:>8.2f} MB"
                )
            logger.warning("====================================")
            logger.warning(
                f"✅ TOTAL ETL TIME: {total_elapsed:.2f} seconds ({total_elapsed/60:.2f} minutes)"
            )

    def cleanup(self):
        for t in self.temp_tables:
            try:
                self.conn.execute(f"DROP TABLE IF EXISTS {t}")
            except Exception as e:
                logger.warning(f"Failed to drop table {t}: {e}")
        self.conn.close()
        for p in self.temp_local_paths:
            if os.path.isdir(p):
                shutil.rmtree(p, ignore_errors=True)
            elif os.path.isfile(p):
                os.remove(p)
        log_memory_usage("After cleanup")


def parse_dynamic_params(param_list):
    param_dict = {}
    for param in param_list:
        match = re.match(r"^([\w_]+)=(.*)$", param)
        if not match:
            raise ValueError(f"Invalid parameter format: '{param}'. Use key=value.")
        param_dict[match.group(1)] = match.group(2)
    return param_dict


def run_etl(config_path, parameters=None, log_level="WARNING"):
    global logger
    logger = configure_logger(log_level)
    DuckDBETL(config_path, parameters).run()


logger = configure_logger()

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--param", action="append", default=[])
    parser.add_argument("--log-level", default="WARNING")
    args = parser.parse_args()

    logger = configure_logger(args.log_level)
    params = parse_dynamic_params(args.param)
    DuckDBETL(args.config, params).run()
