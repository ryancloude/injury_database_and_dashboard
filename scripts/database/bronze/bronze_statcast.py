"""
Bronze-layer ingestion for MLB Statcast via pybaseball.


Responsibilities
- Determine an incremental start date (with a configurable lookback window)
- Page through calendar ranges in small API chunks
- Infer/cast Pandas dtypes for a first-pass schema
- Create/alter Postgres bronze tables (+ matching _staging tables)
- Bulk load with COPY; then MERGE staging → target (idempotent upsert)
"""
from pybaseball import statcast
from sqlalchemy import create_engine, text, inspect
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
import time
import os
import warnings
import io
from pandas.api.types import is_float_dtype

def get_start_date(engine, base: str, column_name: str, window: int, schema:str, default_start=datetime(2015,1,1)):
    """
    Compute the first date to request for an incremental load.


    Looks up the max date in the target table and moves back `window` days to allow for
    late-arriving updates/corrections. If the table is missing or empty, returns
    `default_start`.

    Args:
    engine: SQLAlchemy engine connected to the database.
    base: Base table name (without schema). (ex. 'statcast')
    column_name: Date column in the target table (ex. 'game_date').
    window: Number of days to back off from the max date (ex. 7).
    schema: Schema name (ex. 'bronze').
    default_start: Fallback start date for first-ever load.


    Returns:
    datetime: Start date for this ingestion run.
    """
    with engine.connect() as conn:
        inspector = inspect(conn)
        table_exist = inspector.has_table(base, schema)
        if table_exist:
            result = conn.execute(text(f"select max({column_name}) from {schema}.{base}"))
            value =  result.scalar()
            if value is not None:
                #If the column is text in DB, otherwise assume it's a date/datetime
                if type(value) == str:
                    value = datetime.strptime(value, "%Y-%m-%d")             
                start_date = value - timedelta(days=window)
            else:
                return default_start
        else:
            return default_start
        return start_date


def get_date_chunks(start_date: datetime.date, end_date: datetime.date, chunk_size: int):
    """Split a date range into inclusive [start, end] chunks.

    Designed to respect API rate limits by making many small requests.

    Args:
    start_date: First date to request.
    end_date: Last date to request.
    chunk_size: Number of days per chunk (ex. 5).

    Returns:
    list[[str, str]]: pairs of dates as strings in (YYYY-MM-DD) format for each chunk.
    """
    chunks = []
    current_date = start_date
    while current_date <= end_date:
        chunk_end = min(current_date + timedelta(days=chunk_size-1), end_date)
        chunks.append((current_date.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d')))
        current_date += timedelta(days=chunk_size)
    return chunks


def infer_and_cast_dtypes_psql(
    df: pd.DataFrame):
    """Best-effort dtype inference for a bronze landing DataFrame.


    Strategy
    - Recognize booleans from common tokens (true/false, yes/no, t/f, y/n)
    - Parse ISO dates (YYYY-MM-DD) and UTC datetimes (YYYY-MM-DDTHH:MM:SSZ)
    - Identify integers when all numeric and no decimals are present
    - Mark existing numeric dtypes as NUMERIC (see note below)
    - Fallback to pandas string dtype (TEXT in Postgres)


    Returns
    pd.DataFrame, {str, str}: dataframe with infered types, dictionary with column name: postgresql type
    """
    columns_and_types = {}

    for col in df.columns:
        s = df[col]
        TRUE_TOKENS = {"true", "t", "yes", "y"}
        FALSE_TOKENS = {"false", "f", "no", "n"}
        # 1. Boolean detection
        if s.dropna().astype(str).str.strip().str.lower().isin(TRUE_TOKENS | FALSE_TOKENS).all():
            df[col] = (
                s.astype("string").str.strip().str.lower()
                .map(lambda x: True if x in TRUE_TOKENS else False if x in FALSE_TOKENS else pd.NA)
                .astype("boolean")
            )
            columns_and_types[col] = "boolean"
            continue

        # 2. Date / datetime detection
        try:
            df[col] = pd.to_datetime(df[col], errors='raise', format='%Y-%m-%d')
            columns_and_types[col] = 'date'
            continue
        except:
            try:
                    df[col] = pd.to_datetime(df[col], errors='raise', format='%Y-%m-%dT%H:%M:%SZ')
                    columns_and_types[col] = 'timestamp with time zone'
                    continue
            except:
                pass

            # 3. Integer detection (only if all numeric and no decimals)
            try:
                nums = pd.to_numeric(df[col], errors='coerce')
                if not df[col].dropna().empty and (nums.dropna() % 1 == 0).all():
                    df[col] = df[col].astype("Int64")
                    columns_and_types[col] = "bigint"
                    continue
            except:
                 pass

            #float -> double precision
            if is_float_dtype(df[col].dtype):
                 columns_and_types[col] = 'double precision'
                 continue 

            # 5. Default → TEXT
            df[col] = s.astype('string')
            columns_and_types[col] = "text"

    return df, columns_and_types

def get_table_columns_and_types(engine, base: str, schema: str):

    """Fetch current column -> data_type mapping from information_schema.
    Queries the *staging* table (`{base}_staging`). Returns None if it doesn't exist.
    
    Args:
    engine: SQLAlchemy engine.
    base: Base table name (without schema).
    schema: Schema name.


    Returns:
    {str: str} or None: {column : data_type} (as reported by Postgres), or None if
    the staging table has not been created yet.
    """
    schema = schema
    sql = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :tbl
        ORDER BY ordinal_position
    """
    columns_and_types = {}
    with engine.connect() as conn:
        inspector = inspect(conn)
        table_exist = inspector.has_table(f'{base}_staging', schema)
        if table_exist:
            rows = conn.execute(text(sql), {"schema": schema, "tbl": f'{base}_staging'}).fetchall()
            for row in rows:
                columns_and_types[row[0]] = row[1]
            return columns_and_types
        else:
            return None

def fast_copy_from(df: pd.DataFrame, schema: str, base: str, engine):
    """Bulk load a DataFrame into `{schema}.{base}_staging` using COPY FROM STDIN.


    Implementation details
    - Writes the DataFrame to an in-memory CSV (no header) and streams it to Postgres.
    - Assumes the staging table exists and its column order and types matches the DataFrame order.

    Args:
    df: Data to insert (columns and types must match staging table order).
    schema: Schema name.
    base: Base table name (without schema).
    engine: SQLAlchemy engine.

    Returns:
    None
    """
    conn = engine.raw_connection()
    cursor = conn.cursor()
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    try:
        cursor.copy_expert(f"COPY {schema}.{base}_staging FROM STDIN WITH CSV", buffer)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[❌] fast_copy_from failed: {e}")

        
    finally:
        cursor.close()
        conn.close()
    return

def alter_table(df: pd.DataFrame, existing_columns_and_types: dict, chunk_columns_and_types: dict, base: str, schema: str, engine):
    """Align a new chunk's schema to the current table, and add any new columns.

    Behavior
    - Computes the *missing columns* (present in table but not in chunk, or present with a different type)
    - Casts/creates those columns in the DataFrame to match the table's types
    - Adds truly new columns to both target and staging tables (idempotent ALTER)

    Args:
    df: Incoming chunk DataFrame.
    existing_columns_and_types: Current `{col: pg_type}` for the table.
    chunk_columns_and_types: Types inferred for the chunk.
    base: Base table name.
    schema: Schema name.
    engine: SQLAlchemy engine.


    Returns:
    pd.DataFrame, {str: str}: Aligned DataFrame, updated dictionary containg column names and types
    """  
    # Compute differences as a set of (column, type) pairs
    missing_columns = list(set(existing_columns_and_types.items()) -  set(chunk_columns_and_types.items()))
    TRUE_TOKENS = {"true", "t", "yes", "y"}
    FALSE_TOKENS = {"false", "f", "no", "n"}
    for col, psql_type in missing_columns:
        # Normalize the chunk's type map to table expectation
        chunk_columns_and_types[col] = psql_type
        # If the column exists in the chunk, it needs casting; otherwise create a NULL filler
        if col in df:
            if psql_type == 'bigint':
                   df[col] = pd.to_numeric(df[col], errors='coerce').round().astype("Int64")

            elif psql_type == 'boolean':
                df[col] = (
                df[col].astype("string").str.strip().str.lower()
                .map(lambda x: True if x in TRUE_TOKENS else False if x in FALSE_TOKENS else pd.NA)
                .astype("boolean"))
            
            elif psql_type == 'date':
                df[col] = pd.to_datetime(df[col], errors='coerce', format='%Y-%m-%d')
        
            elif psql_type == 'timestamp with time zone':
                df[col] = pd.to_datetime(df[col], errors='coerce', format='%Y-%m-%dT%H:%M:%SZ')

            elif psql_type == 'double precision':
                df[col] = df[col].astype(float)
            else:
                df[col] = df[col].astype('string')
        # column truly missing in this chunk        
        else:
            df[col] = None

    # Adds new columns to both staging and target (idempotent)        
    new_columns = set(chunk_columns_and_types.items()) -  set(existing_columns_and_types.items())
    for col, type in new_columns:
        existing_columns_and_types[col] = chunk_columns_and_types[col]
        print(f"[INFO] Adding new column '{col}' ({type}) to table {schema}.{base} and {schema}.{base}_staging")
        alter_stg_stmt = text(f'ALTER TABLE {schema}.{base}_staging ADD COLUMN IF NOT EXISTS "{col}" {chunk_columns_and_types[col]};')
        alter_trgt_stmt = text(f'ALTER TABLE {schema}.{base} ADD COLUMN IF NOT EXISTS "{col}" {chunk_columns_and_types[col]};')
        with engine.begin() as conn:
            conn.execute(alter_stg_stmt)
            conn.execute(alter_trgt_stmt)
    return df, existing_columns_and_types


def truncate_staging(engine, schema: str, base: str):
    """Remove all rows from `{schema}.{base}_staging` (fast TRUNCATE)."""
    with engine.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE {schema}.{base}_staging;'))


def ensure_bronze_tables_from_df(df: pd.DataFrame, schema: str, engine, base: str, primary_keys=None):
    """Ensure the bronze target and staging tables exist and have ingestion columns.

    Creates `{schema}.{base}` and `{schema}.{base}_staging` if they do not exist using the
    DataFrame's header (column names only). Adds ingestion metadata columns and
    a primary key constraint on the target table.

    Args:
    df: Any non-empty DataFrame with the desired column set.
    schema: Schema name.
    engine: SQLAlchemy engine.
    base: Base table name.
    primary_keys: Optional list of column names to define a PK on the target.
    """
    # 1) Ensure schema exists
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema};'))

    # 2) Create target & staging (if they don't exist) using DataFrame header
    try:
        df.head(0).to_sql(base, engine, if_exists="append", index=False, schema=schema)
    except Exception:
        pass  # likely already exists
    try:
        df.head(0).to_sql(f'{base}_staging', engine, if_exists="append", index=False, schema=schema)
    except Exception:
        pass  # likely already exists

    # 3) Add ingestion columns & PK to target (idempotent ALTERs)
    with engine.begin() as conn:
        # ingestion timestamps
        conn.execute(text(f'ALTER TABLE {schema}.{base} '
                          f'ADD COLUMN IF NOT EXISTS first_ingested_at TIMESTAMPTZ DEFAULT now();'))
        conn.execute(text(f'ALTER TABLE {schema}.{base} '
                          f'ADD COLUMN IF NOT EXISTS last_ingested_at  TIMESTAMPTZ DEFAULT now();'))
        if primary_keys is not None:
            pk = ",".join(f'"{col}"' for col in primary_keys)
            try:
                conn.execute(text(
                    f'ALTER TABLE {schema}.{base} '
                    f'ADD CONSTRAINT {base}_pk PRIMARY KEY ({pk});'
                ))
                print(f"[INFO] Added PK on ({pk}) for {schema}.{base}")
            except Exception:
                # constraint already exists
                pass
    return

def create_indexes(engine, indexes: dict, schema: str, base: str):
    """Create helpful secondary indexes if missing.


    Args:
    engine: SQLAlchemy engine.
    indexes: Mapping of logical name -> list of column names (in order).
    schema: Schema name.
    base: Base table name.
    """
    with engine.begin() as conn:
        for name, columns in indexes.items():
            conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{name} ON {schema}.{base} ({",".join(columns)});'))
    return

def merge_staging_into_target(schema: str, base: str,engine, primary_keys: list, columns: list):
    """Idempotently upsert from staging -> target using MERGE.

    Rows with NULLs in any primary-key column are skipped. On match, updates non-PK columns
    and refreshes `last_ingested_at`. On no match, inserts the full row (letting defaults
    populate ingestion timestamps).

    Args:
    schema: Schema name.
    base: Base table name.
    engine: SQLAlchemy engine.
    primary_keys: List of PK column names.
    columns: All column names to insert/update 
    always set `last_ingested_at`).
    """
    non_pk = [c for c in columns if c not in primary_keys and c not in ("first_ingested_at", "last_ingested_at")]

    # INSERT columns = common + ingestion columns (let defaults handle first_ingested_at/last_ingested_at)
    insert_cols = ", ".join([f'"{c}"' for c in columns])
    insert_vals = ", ".join([f's."{c}"' for c in columns])

    merge_sql = f"""
        MERGE INTO {schema}.{base} AS t
        USING (
        select *
        from {schema}.{base}_staging
        where {" and ".join([f'{c} is not null' for c in primary_keys])}) AS s
        ON ({" AND ".join([f't.{c} = s.{c}' for c in primary_keys])})
        WHEN MATCHED THEN
          UPDATE SET {", ".join([f'{c} = COALESCE(s.{c}, t.{c})' for c in non_pk])}, "last_ingested_at" = now()
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols}) VALUES ({insert_vals});"""
    
    with engine.begin() as conn:
        conn.execute(text(merge_sql))




def create_statcast(start_date: datetime.date, end_date:datetime.date, chunk_size:int, \
                    engine, base: str, schema: str, primary_keys: list, indexes: dict):
    """End-to-end Statcast ingestion into bronze.

    For each season window between `start_date` and `end_date`:
    1) Build API date chunks
    2) Download data via `pybaseball.statcast`
    3) Normalize: lower-case columns, drop all-null columns, de-dup on PK
    4) On first chunk: infer schema and ensure tables exist; create indexes
    5) Align schemas as needed; bulk COPY into staging; MERGE into target; TRUNCATE staging

    Args:
    start_date: First date to ingest (will be clamped to Feb 1 of the first season).
    end_date: Last date to ingest (clamped to Nov 30 for intermediate seasons).
    chunk_size: Number of days per Statcast API request).
    engine: SQLAlchemy engine.
    base: Base table name ('statcast').
    schema: Schema name ('bronze').
    primary_keys: Natural key columns, e.g. ['game_pk','at_bat_number','pitch_number'].
    indexes: Mapping of helpful index names -> column lists.
    """
    first_chunk = True
    for year in range(start_date.year, end_date.year + 1):
        print(f"\n Year: {year}")
        season_start = datetime(year, 2, 1)
        year_start = max(season_start, start_date) if year == start_date.year else season_start
        year_end = end_date if year == end_date.year else datetime(year, 11, 30)
        date_chunks = get_date_chunks(year_start, year_end, chunk_size)       
        for chunk_start, chunk_end in tqdm(date_chunks, desc=f"fetching {year}"):
            try:
                # Silence known warnings from pybaseball
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=FutureWarning)
                    df = statcast(start_dt=chunk_start, end_dt=chunk_end)
                if df.empty:
                    continue
                df = df.reset_index(drop=True)
                df.columns = df.columns.map(str).str.lower()
                df = df.dropna(axis=1, how='all')
                #Rows with duplicate pirmary keys are dropped, last row with primary key is kept
                df = df.drop_duplicates(subset=primary_keys, keep='last')
                if first_chunk:
                    #First chunk gets column names and types from table
                    existing_columns_and_types = get_table_columns_and_types(engine=engine, base=base, schema=schema)
                    #If the table does not exist yet, existing column names and types are set by first chunk of data
                    if existing_columns_and_types is None:
                        df, existing_columns_and_types = infer_and_cast_dtypes_psql(df=df)
                        chunk_columns_and_types = existing_columns_and_types
                    else:
                        df, chunk_columns_and_types = infer_and_cast_dtypes_psql(df=df)
                    ensure_bronze_tables_from_df(df, base=base, schema=schema, engine=engine, primary_keys=primary_keys)
                    truncate_staging(engine=engine, schema=schema, base=base)
                    create_indexes(engine=engine, indexes=indexes, schema=schema, base=base)
                    first_chunk = False
                else:
                    df, chunk_columns_and_types = infer_and_cast_dtypes_psql(df=df)
                # If schema has changed, update table structure
                if chunk_columns_and_types != existing_columns_and_types:
                    df, existing_columns_and_types = alter_table(df, base=base, schema=schema, engine=engine, \
                                                                 chunk_columns_and_types=chunk_columns_and_types, \
                                                                existing_columns_and_types=existing_columns_and_types)
                try:
                    #Orders the columns to match order in table
                    df = df[list(existing_columns_and_types.keys())]    
                    fast_copy_from(df, schema=schema, base=base, engine=engine)
                    merge_staging_into_target(base=base, engine=engine, primary_keys=primary_keys, \
                                              columns=list(existing_columns_and_types.keys()), schema=schema)
                    print(f"Inserted {len(df)} rows from {chunk_start} to {chunk_end}")
                    truncate_staging(engine, schema=schema, base=base)
                except Exception as e:
                    print(f"[WARNING] fast_copy_from failed: {e}")
                    truncate_staging(engine, schema=schema, base=base)
            except Exception as e:
                print(f"Failed for {chunk_start} to {chunk_end}: {e}")
                time.sleep(2)
    return

if __name__ == "__main__":
    # Load PostgreSQL connection string from environment variable
    BASEBALL_URL = os.environ['BASEBALL_URL']
    engine = create_engine(BASEBALL_URL)

    base_table_name = 'statcast'
    schema = 'bronze'
    indexes = {"pitcher_game": ['pitcher','game_date'], "pitcher_pitch_type_game":['game_date','pitcher','pitch_type']}
    primary_key = ['game_pk', 'at_bat_number', 'pitch_number']

    start_date = get_start_date(engine=engine, base=base_table_name, column_name='game_date',schema=schema, window=7)
    end_date = datetime.today() - timedelta(days=1)
    chunk_size = 5 # Days per chunk (Statcast has request limits)

    
    # Test DB connection before starting extraction
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            print("✅ Connected to PostgreSQL!")
            print(f"PostgreSQL version: {result.fetchone()[0]}")
    except Exception as e:
        print("Failed to connect to PostgreSQL:")
        print(e)
        exit()
    # Begin Statcast data ingestion
    create_statcast(start_date=start_date, end_date=end_date, chunk_size=chunk_size, \
                    engine=engine, base=base_table_name, schema=schema, primary_keys=primary_key, indexes=indexes)