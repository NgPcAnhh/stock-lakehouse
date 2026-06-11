from abc import ABC, abstractmethod
import asyncpg
import clickhouse_connect
import asyncio
from typing import Any, Dict, List, Optional

class DatabaseEngine(ABC):
    @abstractmethod
    async def test_connection(self, config: Dict[str, Any]) -> bool:
        pass

    @abstractmethod
    async def get_databases(self, config: Dict[str, Any]) -> List[str]:
        pass

    @abstractmethod
    async def get_metadata(self, config: Dict[str, Any], target_db: Optional[str] = None) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def execute_query(self, config: Dict[str, Any], sql: str, database: Optional[str] = None, schema: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        pass

class PostgresEngine(DatabaseEngine):
    async def _get_connection(self, config: Dict[str, Any], database: Optional[str] = None):
        return await asyncpg.connect(
            user=config['username'],
            password=config.get('password'),
            database=database or config.get('database_name') or 'postgres',
            host=config['host'],
            port=config.get('port') or 5432,
            timeout=10
        )

    async def test_connection(self, config: Dict[str, Any]) -> bool:
        try:
            conn = await self._get_connection(config)
            await conn.close()
            return True
        except Exception as e:
            print(f"Postgres connection failed: {e}")
            return False

    async def get_databases(self, config: Dict[str, Any]) -> List[str]:
        conn = await self._get_connection(config)
        try:
            rows = await conn.fetch("SELECT datname FROM pg_database WHERE datistemplate = false;")
            return [row['datname'] for row in rows]
        finally:
            await conn.close()

    async def get_metadata(self, config: Dict[str, Any], target_db: Optional[str] = None) -> Dict[str, Any]:
        connect_db = target_db or config.get('database_name') or 'postgres'
        conn = await self._get_connection(config, database=connect_db)
        try:
            query = """
                SELECT table_schema, table_name, column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'system') 
                ORDER BY table_schema, table_name, ordinal_position;
            """
            rows = await conn.fetch(query)
            
            schemas_dict = {}
            for row in rows:
                schema_name = row['table_schema']
                table_name = row['table_name']
                col_name = row['column_name']
                col_type = row['data_type']
                
                if schema_name not in schemas_dict:
                    schemas_dict[schema_name] = {}
                if table_name not in schemas_dict[schema_name]:
                    schemas_dict[schema_name][table_name] = []
                    
                schemas_dict[schema_name][table_name].append({
                    "name": col_name,
                    "type": col_type
                })
                
            result_schemas = []
            for s_name, tables in schemas_dict.items():
                result_tables = []
                for t_name, cols in tables.items():
                    result_tables.append({"name": t_name, "columns": cols})
                result_schemas.append({"name": s_name, "tables": result_tables})
                
            return {
                "database": connect_db,
                "schemas": result_schemas
            }
        finally:
            await conn.close()

    async def execute_query(self, config: Dict[str, Any], sql: str, database: Optional[str] = None, schema: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        conn = await self._get_connection(config, database=database)
        try:
            if schema:
                safe_schema = schema.replace('"', '""')
                await conn.execute(f'SET search_path TO "{safe_schema}"')
            else:
                await conn.execute("SET search_path TO public")
            
            clean_sql = sql.strip()
            if clean_sql.endswith(';'):
                clean_sql = clean_sql[:-1].strip()

            actual_limit = min(limit or 100, 100000)
            wrapped_sql = f"SELECT * FROM ({clean_sql}) AS subquery LIMIT {actual_limit}"
            
            records = await conn.fetch(wrapped_sql)
            
            if not records:
                return {"columns": [], "rows": []}
            
            columns = [{"name": key, "type": "string"} for key in records[0].keys()]
            rows = [dict(record) for record in records]
            return {"columns": columns, "rows": rows}
        finally:
            await conn.close()

class ClickHouseEngine(DatabaseEngine):
    def _get_client(self, config: Dict[str, Any], database: Optional[str] = None):
        return clickhouse_connect.get_client(
            host=config['host'],
            port=config.get('port') or 8123,
            username=config.get('username') or 'default',
            password=config.get('password') or '',
            database=database or config.get('database_name') or 'default'
        )

    async def test_connection(self, config: Dict[str, Any]) -> bool:
        try:
            # clickhouse-connect is synchronous, we wrap it in a thread
            def _test():
                client = self._get_client(config)
                client.command("SELECT 1")
                client.close()
                return True
            
            return await asyncio.to_thread(_test)
        except Exception as e:
            print(f"ClickHouse connection failed: {e}")
            return False

    async def get_databases(self, config: Dict[str, Any]) -> List[str]:
        def _get():
            client = self._get_client(config)
            result = client.query("SHOW DATABASES")
            databases = [row[0] for row in result.result_rows if row[0] not in ('system', 'information_schema', 'INFORMATION_SCHEMA')]
            client.close()
            return databases
            
        return await asyncio.to_thread(_get)

    async def get_metadata(self, config: Dict[str, Any], target_db: Optional[str] = None) -> Dict[str, Any]:
        connect_db = target_db or config.get('database_name') or 'default'
        
        def _get():
            client = self._get_client(config, database=connect_db)
            query = f"""
                SELECT table, name, type 
                FROM system.columns 
                WHERE database = '{connect_db}'
                ORDER BY table, position
            """
            result = client.query(query)
            
            tables_dict = {}
            for row in result.result_rows:
                table_name = row[0]
                col_name = row[1]
                col_type = row[2]
                
                if table_name not in tables_dict:
                    tables_dict[table_name] = []
                
                tables_dict[table_name].append({
                    "name": col_name,
                    "type": col_type
                })
            
            result_tables = []
            for t_name, cols in tables_dict.items():
                result_tables.append({"name": t_name, "columns": cols})
                
            client.close()
            return {
                "database": connect_db,
                "schemas": [{"name": "default", "tables": result_tables}]
            }
            
        return await asyncio.to_thread(_get)

    async def execute_query(self, config: Dict[str, Any], sql: str, database: Optional[str] = None, schema: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        connect_db = database or config.get('database_name') or 'default'
        
        def _exec():
            client = self._get_client(config, database=connect_db)
            
            clean_sql = sql.strip()
            if clean_sql.endswith(';'):
                clean_sql = clean_sql[:-1].strip()

            actual_limit = min(limit or 100, 100000)
            wrapped_sql = f"SELECT * FROM ({clean_sql}) LIMIT {actual_limit}"
            
            result = client.query(wrapped_sql)
            
            columns = [{"name": name, "type": str(type_)} for name, type_ in zip(result.column_names, result.column_types)]
            rows = []
            for row in result.result_rows:
                row_dict = {}
                for i, name in enumerate(result.column_names):
                    row_dict[name] = row[i]
                rows.append(row_dict)
                
            client.close()
            return {"columns": columns, "rows": rows}
            
        return await asyncio.to_thread(_exec)

import aiomysql
import pymssql

class MySQLEngine(DatabaseEngine):
    async def _get_connection(self, config: Dict[str, Any], database: Optional[str] = None):
        return await aiomysql.connect(
            host=config['host'],
            port=config.get('port') or 3306,
            user=config['username'],
            password=config.get('password'),
            db=database or config.get('database_name'),
            autocommit=True
        )

    async def test_connection(self, config: Dict[str, Any]) -> bool:
        try:
            conn = await self._get_connection(config)
            conn.close()
            return True
        except Exception as e:
            print(f"MySQL connection failed: {e}")
            return False

    async def get_databases(self, config: Dict[str, Any]) -> List[str]:
        conn = await self._get_connection(config)
        try:
            async with conn.cursor() as cur:
                await cur.execute("SHOW DATABASES")
                rows = await cur.fetchall()
                return [row[0] for row in rows if row[0] not in ('information_schema', 'mysql', 'performance_schema', 'sys')]
        finally:
            conn.close()

    async def get_metadata(self, config: Dict[str, Any], target_db: Optional[str] = None) -> Dict[str, Any]:
        connect_db = target_db or config.get('database_name')
        conn = await self._get_connection(config, database=connect_db)
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                query = """
                    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE 
                    FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = %s
                    ORDER BY TABLE_NAME, ORDINAL_POSITION
                """
                await cur.execute(query, (connect_db,))
                rows = await cur.fetchall()
                
                tables_dict = {}
                for row in rows:
                    t_name = row['TABLE_NAME']
                    if t_name not in tables_dict:
                        tables_dict[t_name] = []
                    tables_dict[t_name].append({
                        "name": row['COLUMN_NAME'],
                        "type": row['DATA_TYPE']
                    })
                
                result_tables = [{"name": t, "columns": c} for t, c in tables_dict.items()]
                return {
                    "database": connect_db,
                    "schemas": [{"name": connect_db, "tables": result_tables}]
                }
        finally:
            conn.close()

    async def execute_query(self, config: Dict[str, Any], sql: str, database: Optional[str] = None, schema: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        connect_db = database or config.get('database_name')
        conn = await self._get_connection(config, database=connect_db)
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                clean_sql = sql.strip().rstrip(';')
                actual_limit = min(limit or 100, 100000)
                wrapped_sql = f"SELECT * FROM ({clean_sql}) AS subquery LIMIT {actual_limit}"
                
                await cur.execute(wrapped_sql)
                rows = await cur.fetchall()
                
                if not rows:
                    return {"columns": [], "rows": []}
                
                columns = [{"name": key, "type": "string"} for key in rows[0].keys()]
                return {"columns": columns, "rows": rows}
        finally:
            conn.close()

class SQLServerEngine(DatabaseEngine):
    def _get_conn_params(self, config: Dict[str, Any], database: Optional[str] = None):
        return {
            'server': config['host'],
            'port': config.get('port') or 1433,
            'user': config['username'],
            'password': config.get('password'),
            'database': database or config.get('database_name') or 'master',
            'timeout': 10,
            'as_dict': True
        }

    async def test_connection(self, config: Dict[str, Any]) -> bool:
        try:
            def _test():
                conn = pymssql.connect(**self._get_conn_params(config))
                conn.close()
                return True
            return await asyncio.to_thread(_test)
        except Exception as e:
            print(f"SQL Server connection failed: {e}")
            return False

    async def get_databases(self, config: Dict[str, Any]) -> List[str]:
        def _get():
            conn = pymssql.connect(**self._get_conn_params(config))
            cur = conn.cursor()
            cur.execute("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')")
            dbs = [row[0] for row in cur.fetchall()]
            conn.close()
            return dbs
        return await asyncio.to_thread(_get)

    async def get_metadata(self, config: Dict[str, Any], target_db: Optional[str] = None) -> Dict[str, Any]:
        connect_db = target_db or config.get('database_name')
        def _get():
            conn = pymssql.connect(**self._get_conn_params(config, database=connect_db))
            cur = conn.cursor(as_dict=True)
            query = """
                SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE 
                FROM INFORMATION_SCHEMA.COLUMNS 
                ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
            """
            cur.execute(query)
            rows = cur.fetchall()
            
            schemas_dict = {}
            for row in rows:
                s_name = row['TABLE_SCHEMA']
                t_name = row['TABLE_NAME']
                if s_name not in schemas_dict: schemas_dict[s_name] = {}
                if t_name not in schemas_dict[s_name]: schemas_dict[s_name][t_name] = []
                schemas_dict[s_name][t_name].append({
                    "name": row['COLUMN_NAME'],
                    "type": row['DATA_TYPE']
                })
            
            result_schemas = []
            for s, tables in schemas_dict.items():
                result_tables = [{"name": t, "columns": c} for t, c in tables.items()]
                result_schemas.append({"name": s, "tables": result_tables})
            
            conn.close()
            return {"database": connect_db, "schemas": result_schemas}
        return await asyncio.to_thread(_get)

    async def execute_query(self, config: Dict[str, Any], sql: str, database: Optional[str] = None, schema: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        connect_db = database or config.get('database_name')
        def _exec():
            conn = pymssql.connect(**self._get_conn_params(config, database=connect_db))
            cur = conn.cursor(as_dict=True)
            
            clean_sql = sql.strip().rstrip(';')
            actual_limit = min(limit or 100, 100000)
            # SQL Server uses TOP for limit
            wrapped_sql = f"SELECT TOP {actual_limit} * FROM ({clean_sql}) AS subquery"
            
            cur.execute(wrapped_sql)
            rows = cur.fetchall()
            
            if not rows:
                return {"columns": [], "rows": []}
            
            columns = [{"name": key, "type": "string"} for key in rows[0].keys()]
            conn.close()
            return {"columns": columns, "rows": rows}
        return await asyncio.to_thread(_exec)

def get_engine(db_type: str) -> DatabaseEngine:
    if db_type == 'postgres':
        return PostgresEngine()
    elif db_type == 'clickhouse':
        return ClickHouseEngine()
    elif db_type == 'mysql':
        return MySQLEngine()
    elif db_type == 'sqlserver':
        return SQLServerEngine()
    else:
        raise ValueError(f"Unsupported database type: {db_type}")
