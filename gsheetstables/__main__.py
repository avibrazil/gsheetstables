#!/usr/bin/env python

# Used only when testing locally
# import sys
# sys.path.insert(0,'..')

import sys
import os
import math
import datetime
import textwrap
import argparse
import pathlib
import logging
import base64
import json

import dotmap
import cryptography.hazmat.primitives.serialization
import sqlalchemy
import jinja2
import pandas
import gsheetstables


default_identity_file = pathlib.Path.home() / 'service_account.json'

def prepare_logging(verbose: int):
    level = logging.WARNING
    if verbose == 1:
        level = logging.INFO
    elif verbose >= 2:
        level = logging.DEBUG

    logging.basicConfig(
        level=level,  # default level
        # format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    loggers=[
        logging.getLogger(__name__),
        logging.getLogger('gsheetstables'),
    ]

    for logger in loggers:
        logger.setLevel(level)

    # Return the main logger to be throughout this program
    return loggers[0]



def prepare_args():
    parser = argparse.ArgumentParser(
        prog='gsheetstables2db',
        description='Copy the Tables (only Tables) of a Google Spreadsheet to a SQL database'
    )

    parser.add_argument(
        '--sheet', '-s',
        dest='gsheet',
        required=True,
        help='ID of Google Sheet to retrieve Tables.'
    )

    parser.add_argument(
        '--db',
        dest='db_url',
        required=False,
        default='sqlite:///tables.sqlite',
        help='SQLAlchemy URL of database where tables will be created and maintained. Tables can be written to any SQL database that you have a SQLAlchemy driver installed and permissions to write. Defaults to sqlite:///tables.sqlite'
    )

    parser.add_argument(
        '--schema',
        dest='db_schema',
        required=False,
        default=None,
        help='Write tables into a specific DB schema, if backend supports schemas'
    )

    parser.add_argument(
        '--table-prefix', '-p',
        dest='table_prefix',
        required=False,
        default='',
        help='Prefix this string to every table name in the target database'
    )

    parser.add_argument(
        '--identity-file', '-i',
        dest='service_account_file',
        required=False,
        default=None,
        help=f'Path to JSON file that contains the private key of account authorized to access the spreadsheet. Download it from Google Cloud Console. Defaults to {default_identity_file}'
    )

    parser.add_argument(
        '--service-account', '-c',
        dest='service_account',
        required=False,
        default=None,
        help='E-mail address of service account as created in Google Cloud Console'
    )

    parser.add_argument(
        '--service-account-private-key', '-m',
        dest='service_account_private_key',
        required=False,
        default=None,
        help='Encoded and encrypted private key. Run me with --identity-file and -vv to see what to pass.'
    )

    parser.add_argument(
        '--rename',
        dest='col_rename',
        required=False,
        default=None,
        help='''Column rename map in JSON. Example: '{"table_1": {"Original column name": "simplified_name",…}, "table_2": {…}}'.'''
    )

    parser.add_argument(
        '--slugify', '-y',
        dest='slugify',
        action=argparse.BooleanOptionalAction,
        required=False,
        default=True,
        help='Slugify, simplify column names to be more database-friendly. Defaults to slugify.'
    )

    parser.add_argument(
        '--keep-snapshots', '-n',
        dest='nsnapshots',
        type=int,
        required=False,
        default=1,
        help='Keep only the last N snapshots of data and delete older ones. Pass 0 to never delete snapshots. Defaults is 1, which keeps only the current state of the GSheet table.'
    )

    parser.add_argument(
        '--sql-pre',
        dest='sql_pre',
        required=False,
        default=None,
        help='SQL script to execute before writing tables to DB. Can be a Jinja template. In case of multi-line script, use the char at --sql-split-char to separate each query. SQL pre, post and actual ETL happen in one single transaction.'
    )

    parser.add_argument(
        '--sql-post',
        dest='sql_post',
        required=False,
        default=None,
        help='SQL script to execute after writing tables to DB. Can be a Jinja template. In case of multi-line script, use the char at --sql-split-char to separate each query. SQL pre, post and actual ETL happen in one single transaction.'
    )

    parser.add_argument(
        '--sql-split-char',
        dest='sql_split_char',
        required=False,
        default=None,
        help='Character that separates single queries on multi-line pre and post SQL scripts. Tip: use unusual unicode chars as §, 𐩕, ꩜ etc'
    )

    parser.add_argument(
        "--dump-table-structure",
        dest='table_structure',
        type=pathlib.Path,
        default=None,
        help='File name to dump the CREATE TABLE statements of all tables created'
    )

    parser.add_argument(
        '--verbose', '-v',
        dest='verbose',
        action="count",
        default=0,
        help='Increase verbosity; use it multiple times'
    )

    return parser.parse_args()


def get_db(db_url, echo=False):
    """
    Returns a SQLAlchemy DB engine already configured with some best practices
    and connection pooling.

    This is a simplified function inspired by
    https://github.com/avibrazil/investorzilla/blob/main/investorzilla/datacache.py
    """

    engine_config_sets=dict(
        # Documentation for all these SQLAlchemy pool control parameters:
        # https://docs.sqlalchemy.org/en/14/core/engines.html#engine-creation-api

        DEFAULT=dict(
            # QueuePool config for a real database
            poolclass         = sqlalchemy.pool.QueuePool,

            # 5 is the default.
            pool_size         = 2,

            # Default here was 10, which might be low sometimes, so
            # increase to some big number in order to never let the
            # QueuePool be a bottleneck.
            max_overflow      = 50,

            # Debug connection and all queries
            echo              = echo
        ),
        sqlite=dict(
            # SQLite doesn’t support concurrent writes, so we‘ll amend
            # the DEFAULT configuration to make the pool work with only
            # 1 simultaneous connection. Since Investorzilla is agressively
            # parallel and requires a DB service that can be used in
            # parallel (regular DBs), the simplicity and portability
            # offered by SQLite for a light developer laptop has its
            # tradeoffs and we’ll have to tweak it to make it usable in
            # a parallel environment even if SQLite is not parallel.

            # A pool_size of 1 allows only 1 simultaneous connection.
            pool_size         = 1,
            max_overflow      = 0,

            # Since we have only 1 stream of work (pool_size=1),
            # we need to put a hold on other DB requests that arrive
            # from other parallel tasks. We do this putting a high value
            # on pool_timeout, which controls the number of seconds to
            # wait before giving up on getting a connection from the
            # pool.
            pool_timeout      = 3600.0,

            # Debug connection and all queries
            # echo              = True
        ),
    )

    # Start with a default config
    engine_config=engine_config_sets['DEFAULT'].copy()

    # Add engine-specific configs
    for dbtype in engine_config_sets.keys():
        # Extract from engine_config_sets configuration specific
        # for each DB type
        if dbtype in db_url:
            engine_config.update(engine_config_sets[dbtype])

    logger.debug(f"Creating a DB engine on {db_url}")

    return sqlalchemy.create_engine(
        url = db_url,
        **engine_config
    )


def encode_identity(identity_file, logger):
    i=json.load(open(identity_file))

    enc=cryptography.hazmat.primitives.serialization.BestAvailableEncryption(os.getenv('USER').encode())

    k=cryptography.hazmat.primitives.serialization.load_pem_private_key(
        i['private_key'].encode(),
        password=None
    )

    payload=base64.b64encode(
        k.private_bytes(
            encoding             = cryptography.hazmat.primitives.serialization.Encoding.DER,
            format               = cryptography.hazmat.primitives.serialization.PrivateFormat.PKCS8,
            encryption_algorithm = enc
        )
    ).decode()

    logger.debug(
        f"""Next time pass the following in the command line to avoid the {identity_file} identity file:""" +
        f"""--service-account {i['client_email']} --service-account-private-key {payload}"""
    )


def decode_identity(payload):
    return (
        cryptography.hazmat.primitives.serialization.load_der_private_key(
            base64.b64decode(payload),
            password=os.getenv('USER').encode()
        )
        .private_bytes(
            encoding              = cryptography.hazmat.primitives.serialization.Encoding.PEM,
            format                = cryptography.hazmat.primitives.serialization.PrivateFormat.PKCS8,
            encryption_algorithm  = cryptography.hazmat.primitives.serialization.NoEncryption()
        )
        .decode()
    )


def chunked_table_write(df,db,schema,target_table,chunk_size=32765):
    """
    Works through a SQLAlchemy or PsycoPG or PostgreSQL INSERT limitation.
    Write the DataFrame to DB in chunks

    chunk_size is number of data cells, not number of rows. So wide tables with
    large number of columns will also benefit.

    The default of 32765 was tested with SQLite and PostgreSQL, and is close to
    the largest number that doesn't break the INSERT.

    Use it like: df.pipe(chunked_table_write, [function parameters])

    Returns the original table in df.
    """
    pages     = math.ceil((df.shape[0] * df.shape[1]) / chunk_size)
    page_size = math.floor(df.shape[0] / pages) + 1

    if pages == 1:
        logger.debug(f"Write table data initially to {target_table}, all data at once.")
    else:
        logger.debug(f"Write table data initially to {target_table}, {pages} pages of {page_size} rows each.")

    for i in range(0, len(df), page_size):
        logger.debug(f"{target_table}: chunk {i}:{page_size + i}")

        (
            df

            # Current page
            .iloc[i:page_size+i]

            # Write to database, finally
            .to_sql(
                target_table,
                schema         = schema,
                if_exists      = 'append',
                method         = 'multi',
                con            = db,
                index          = False
            )
        )

    return df


def sql_script_from_cli(script,sql_split_char,tables,db,name):
    """
    Program can get a full SQL script as CLI argument to pre or post process.
    This function handles this text as it comes from CLI, replaces variables
    with Jinja, breaks it multiple SQL commands/queries and executes one by one.
    """

    if script:
        rendered = (
            jinja2.Template(textwrap.dedent(script))
            .render(tables=tables.tables)
        )

        if sql_split_char and sql_split_char in rendered:
            # Script has multiple commands
            rendered=[
                s1
                for s1 in (
                    s2.strip()
                    for s2 in rendered.split(sql_split_char)
                ) if s1
            ]
        else:
            # Script is only 1 command
            rendered=[rendered]

        for s in rendered:
            ss=' '.join(s.split())
            logger.debug(f"{name}; Computed SQL command: {ss}")
            db.execute(sqlalchemy.text(s))


def get_gsheet_tables(
            gsheet,
            service_account             = None,
            service_account_private_key = None,
            service_account_file        = None,
            slugify                     = True,
            col_rename                  = None,
            show_encoded_identity       = False,
            logger                      = logging.getLogger(__name__)
        ):

    # Prepare common parameters for gsheetstables.GSheetsTables() constructor
    try:
        params = dict(
            gsheetid             = gsheet,
            slugify              = slugify,
            column_rename_map    = json.loads(col_rename) if col_rename else None
        )
    except json.decoder.JSONDecodeError as e:
        # Be more verbose about the errors we control here
        logger.error("Invalid JSON passed to --rename")
        raise

    if service_account is not None and service_account_private_key is not None:
        # Authentication method 1: passed a service account plus private key
        params['service_account']=service_account
        params['private_key']=decode_identity(service_account_private_key)
    elif service_account_file is not None or default_identity_file.exists():
        # Authentication method 2: passed a service account JSON file
        identity=(service_account_file if service_account_file else default_identity_file)

        if show_encoded_identity>=2:
            encode_identity(identity, logger)

        params['service_account_file']=identity
    else:
        logger.error("Either pass an identity file with -i or pure identity with -c and -m. Aborting.")
        sys.exit(1)

    return gsheetstables.GSheetsTables(**params)


def main():
    """
    Many decisions to make about each table retrieved from the GSpreadsheet:

    1.        Table already exists on DB?
    1.1         Timestamp of GSheet is same as last write?
    1.1.1         Skip all DB operations for this table
    1.2         If column change detected
    1.2.1         If not interested in snapshots, simply delete old table
    1.2.2         If we are keeping data snapshots:
    1.2.2.1         Rename old table including old time stamp in its name
    1.2.2.2         Setup everything to continue as if it is a new table
    2.        Write table do DB either as definitive table (new table) or auxiliary for further comparison
    3.        If using auxliary table (old data already existed in DB)
    3.1         Compare old and new (auxiliary) data row by row, cell by cell
    3.2         If data changed append new data to current table
    3.3         Delete auxiliary table
    4.        Keep only {nsapshots} versions of data, purge old versions of data
    """

    # Read environment and command line parameters
    args=prepare_args()

    # Setup logging
    global logger
    logger=prepare_logging(args.verbose)

    tables = get_gsheet_tables(
        gsheet                      = args.gsheet,
        service_account             = args.service_account,
        service_account_private_key = args.service_account_private_key,
        service_account_file        = args.service_account_file,
        slugify                     = args.slugify,
        col_rename                  = args.col_rename,
        show_encoded_identity       = (args.verbose>=2),
        logger                      = logger
    )

    db = get_db(args.db_url, args.verbose>0)

    timestamp_col='_gsheet_utc_timestamp'
    control_cols = [timestamp_col, gsheetstables.GSheetsTables.row_col]

    is_distinct_SQL_operator = dict(
        postgresql = 'IS DISTINCT FROM',
        mysql = '<=>',
    )

    status = dotmap.DotMap(
        created     = [],
        updated     = [],
        unchanged   = [],
        old_purge   = [],
    )

    with db.connect() as db_connection:
        db_connection.begin()

        sql_script_from_cli(
            script          = args.sql_pre,
            sql_split_char  = args.sql_split_char,
            tables          = tables,
            db              = db_connection,
            name            = "Pre ELT script"
        )

        now = datetime.datetime.now(datetime.timezone.utc)
        textual_db_schema = f"{args.db_schema}." if args.db_schema else ''

        for table in tables.tables:
            logger.debug(f"Update logic for DB table {table}...")

            final_table = f"{args.table_prefix}{table}"
            current = f"{textual_db_schema}{final_table}"

            # Define name of DB table where we are going to write data.
            # Defaults to final table name, but this might change...
            target_table = final_table

            table_exists = (
                sqlalchemy.inspect(db_connection)
                .has_table(
                    table_name = final_table,
                    schema     = args.db_schema
                )
            )
            logger.debug(f"Check if target «{current}» exists for {table}: {table_exists}")

            # Check if table in DB needs an update by comparing DB’s table
            # timestamps and spreadsheet last modification time.
            if table_exists:

                current_table_layout_query = (
                    sqlalchemy.text(
                        textwrap.dedent(f"""\
                            SELECT *
                            FROM {current}
                            ORDER BY {timestamp_col} DESC
                            LIMIT 1
                            """
                        )
                    )
                    .compile(
                        dialect=db.dialect,
                        compile_kwargs=dict(literal_binds=True)
                    )
                )

                logger.debug(f"Checking if {current} requires update with query: {current_table_layout_query}")

                current_table_layout = (
                    pandas.read_sql_query(
                        current_table_layout_query,
                        con=db_connection
                    )
                    .assign(
                        **{
                            timestamp_col:
                                lambda table: pandas.to_datetime(
                                    table[timestamp_col],
                                    utc=True
                                )
                        }
                    )
                )
                current_table_timestamp=current_table_layout[timestamp_col].iloc[0]

                # Check if GSheet modification time has changed
                if table_exists:
                    logger.debug(f"GSheet time: {tables.modification_time}")
                    logger.debug(f"DB table last timestamp: {current_table_timestamp}")

                    if tables.modification_time.replace(microsecond=0) > current_table_timestamp:
                        logger.debug(f"Spreadsheet was updated more recently than {current}; row by row comparison triggered.")
                        target_table = f'{final_table}___tmp_'
                    else:
                        # DB already has data with timestamp equal or more
                        # recent than the spreadsheet last modification time.

                        status.unchanged.append(table)

                        logger.debug(f"Table {current} doesn‘t need update in DB.")

                        continue
                else:
                    status.created.append(table)

                # Check if GSheet table columns is different from previous
                # version on DB
                columns_in_only_one_table = (
                    (
                        # Compare columns of current and new table
                        set(current_table_layout.columns) ^
                        set(tables.t(table).columns)
                    ) -
                    # But exclude the control columns from comparison
                    set(control_cols)
                )

                if len(columns_in_only_one_table)>0:
                    # Current and new table have different columns.
                    # Rename or delete current table

                    logger.debug(f"Data layout for «{table}» changed; unmatched columns: {columns_in_only_one_table} ")

                    if args.nsnapshots==1:
                        # If we are not keeping historical data (nsnapshots==1),
                        # don't bother to save previous data, simple delete it.
                        logger.debug(f"Old data layout is incompatible with whats new. Delete it")
                        db_connection.execute(
                           sqlalchemy.text(f"DROP TABLE {current}")
                        )
                    else:
                        # We are keeping historical data (nsnapshots!=1), but
                        # whats new is incompatilbe, so rename current table
                        # with a time tag

                        new_name = "{final_table}__until_{timetag}".format(
                            final_table = final_table,
                            timetag     = (
                                current_table_timestamp
                                .strftime("%Y%m%d%H%M%S")
                            )
                        )

                        # Kinda portable table rename operation
                        if db_connection.dialect.name == "mysql":
                            sql = f"RENAME TABLE {current} TO {textual_db_schema}{new_name}"
                        elif db_connection.dialect.name == "oracle":
                            sql = f"RENAME {final_table} TO {new_name}"
                        else:
                            sql = f"ALTER TABLE {current} RENAME TO {new_name}"

                        logger.warning(f"Old data for «{table}» will be moved to table «{new_name}» due to layout change")

                        db_connection.execute(sqlalchemy.text(sql))

                    target_table = f'{final_table}'

                    # From now on, act as there is no old data
                    table_exists=False




            # At this point we decided that data has to be written to DB. We
            # also know if we need further data comparison (table_exists==True)
            # or if written data is the final data (table_exists==False).

            # Prepare table data to be written to DB
            (
                tables.t(table)

                # Add the timestamp column
                .assign(**{
                    timestamp_col: (
                        (
                            tables.modification_time
                            .astimezone(datetime.timezone.utc)
                        )
                        if tables.modification_time
                        else now
                    ).replace(microsecond=0)
                })

                # Make index (_gsheet_row) a regular column for better control
                .reset_index(drop=False)

                # Get final columns in correct order, with control columns
                # in the begining
                .pipe(
                    lambda table:
                        table[
                            control_cols +
                            [c for c in table.columns if c not in control_cols]
                        ]
                )

                .pipe(
                    chunked_table_write,
                    db                     = db_connection,
                    schema                 = args.db_schema,
                    target_table           = target_table
                )
            )

            # Check if data really changed. This is the hard core data
            # comparison, row by row, cell by cell, executed by the database
            # engine.
            if table_exists:
                if db_connection.dialect.name in is_distinct_SQL_operator.keys():
                    col_compare = ' OR '.join([
                        "(current.{column} {operator} {target}.{column})".format(
                            column=c,
                            target=target_table,
                            operator=is_distinct_SQL_operator[db_connection.dialect.name]
                        )
                        for c in tables.t(table).columns
                        if c not in {gsheetstables.GSheetsTables.row_col}
                    ])
                else:
                    # Database doesn’t have “IS DISTINCT FROM” operator, so safe
                    # comparison is a bit more complex, to handle NULL
                    col_compare = ' OR '.join([
                        (
                            "(" +
                                "(current.{column}             <>  {target}.{column}            ) OR " +
                                "(current.{column} IS     NULL AND {target}.{column} IS NOT NULL) OR " +
                                "(current.{column} IS NOT NULL AND {target}.{column} IS     NULL)"     +
                            ")"
                        ).format(
                            column=c,
                            target=target_table,
                        )
                        for c in tables.t(table).columns
                        if c not in {gsheetstables.GSheetsTables.row_col}
                    ])

                # If the following query returns more than zero lines, table
                # has changed and requires update.
                # Query is a bit too complex to keep compatibility with all DBs,
                # specially those that don’t support FULL OUTER JOIN (MariaDB).
                diff_query = textwrap.dedent(f"""\
                    WITH current AS (
                        SELECT *
                        FROM {textual_db_schema}{final_table}
                        WHERE {timestamp_col} = (
                            SELECT MAX({timestamp_col})
                            FROM {textual_db_schema}{final_table}
                        )
                    ),
                    diff_left AS (
                        SELECT current.{gsheetstables.GSheetsTables.row_col}
                        FROM current
                        LEFT JOIN {textual_db_schema}{target_table}
                        ON current.{gsheetstables.GSheetsTables.row_col} = {textual_db_schema}{target_table}.{gsheetstables.GSheetsTables.row_col}
                        WHERE {textual_db_schema}{target_table}.{gsheetstables.GSheetsTables.row_col} is NULL OR {col_compare}
                        LIMIT 1
                    ),
                    diff_right AS (
                        SELECT {textual_db_schema}{target_table}.{gsheetstables.GSheetsTables.row_col}
                        FROM current
                        RIGHT JOIN {textual_db_schema}{target_table}
                        ON current.{gsheetstables.GSheetsTables.row_col} = {textual_db_schema}{target_table}.{gsheetstables.GSheetsTables.row_col}
                        WHERE current.{gsheetstables.GSheetsTables.row_col} is NULL OR {col_compare}
                        LIMIT 1
                    )
                    SELECT *
                    FROM diff_left
                    UNION
                    SELECT *
                    FROM diff_right
                """)

                logger.debug(f"Compare tables data with query:\n{diff_query}")

                diff = pandas.read_sql_query(diff_query, con=db_connection)

                if len(diff) > 0:
                    # Data of this scpecific table has changed, append to main
                    # table.

                    status.updated.append(table)
                    logger.debug(f"Detected change in data; updating {final_table}")

                    cols=', '.join(
                        list(tables.t(table).columns) +
                        [gsheetstables.GSheetsTables.row_col,timestamp_col]
                    )

                    db_connection.execute(
                        sqlalchemy.text(textwrap.dedent(f"""\
                            INSERT INTO {textual_db_schema}{final_table} ({cols})
                            SELECT {cols} FROM {textual_db_schema}{target_table}
                        """))
                    )
                else:
                    status.unchanged.append(table)
                    logger.debug(f"Data for table {final_table} didn't change; not updating")

                logger.debug(f"Drop auxiliary table {target_table}")
                db_connection.execute(
                   sqlalchemy.text(textwrap.dedent(f"""\
                       DROP TABLE {textual_db_schema}{target_table}
                   """))
                )

            # Delete old table snapshots, keep only args.nsnapshots
            if args.nsnapshots>0:
                logger.debug(f"Delete old snapshots")

                # Do this with 2 queries to be more portable amongst
                # different DBs

                # Discover the oldest allowed snapshot time
                oldest = pandas.read_sql_query(
                    con=db_connection,
                    sql=textwrap.dedent(f"""\
                        WITH
                            too_old AS (
                                SELECT DISTINCT {timestamp_col}
                                FROM {textual_db_schema}{final_table}
                                ORDER BY {timestamp_col} DESC
                                LIMIT {args.nsnapshots}
                                OFFSET {args.nsnapshots}
                            )
                        SELECT {timestamp_col}
                        FROM too_old
                        LIMIT 1
                    """)
                )

                # Delete everything that is older than oldest allowed snapshot
                if len(oldest) > 0:
                    oldest = (
                        oldest.loc[0].values[0]
                        .astype("datetime64[us]")
                        .astype(datetime.datetime)
                        .replace(tzinfo=datetime.timezone.utc)
                    )

                    status.old_purge.append(table)

                    logger.debug(
                        f"Delete anything older than last allowed snapshot: {oldest}"
                    )

                    db_connection.execute(
                        sqlalchemy.text(textwrap.dedent(f"""\
                            DELETE FROM {textual_db_schema}{final_table}
                            WHERE {timestamp_col} <= :time
                            """
                        )),
                        dict(time = oldest)
                    )

            if args.table_structure:
                metadata = sqlalchemy.MetaData()

                table = sqlalchemy.Table(
                    f"{textual_db_schema}{final_table}",
                    metadata,
                    autoload_with=db_connection
                )

                with args.table_structure.open("a", encoding="utf-8") as f:
                    f.write(
                        str(
                            sqlalchemy.schema
                            .CreateTable(table)
                            .compile(dialect=db_connection.dialect)
                        )
                        .rstrip('\n') + ';\n'
                    )

        sql_script_from_cli(
            script          = args.sql_post,
            sql_split_char  = args.sql_split_char,
            tables          = tables,
            db              = db_connection,
            name            = "Post ELT script"
        )

        db_connection.commit()


    db.dispose() # End of database affairs

    # Display some status
    if len(status.created)>0:
        logger.warning(
                     "Tables (re)created: 🧮" + ', 🧮'.join(status.created)
        )
    if len(status.updated)>0:
        logger.warning(
                         "Tables updated: 🧮" + ', 🧮'.join(status.updated)
        )
    if len(status.unchanged)>0:
        logger.info(
                       "Tables unchanged: 🧮" + ', 🧮'.join(status.unchanged)
        )
    if len(status.old_purge)>0:
        logger.warning(
            "Tables freed of old records: 🧮" + ', 🧮'.join(status.old_purge)
        )


if __name__ == "__main__":
    main()
