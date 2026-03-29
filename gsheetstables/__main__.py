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
        # logging.getLogger(__name__),
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
        '--verbose', '-v',
        dest='verbose',
        action="count",
        default=0,
        help='Increase verbosity; use it multiple times'
    )

    return parser.parse_args()


# A simplified function inspired by https://github.com/avibrazil/investorzilla/blob/main/investorzilla/datacache.py
def get_db(db_url, echo=False):
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


def main():
    # Read environment and command line parameters
    args=prepare_args()

    # Setup logging
    global logger
    logger=prepare_logging(args.verbose)

    # Check how we are going to authenticate with Google
    if args.service_account is not None and args.service_account_private_key is not None:
        try:
            tables = gsheetstables.GSheetsTables(
                gsheetid             = args.gsheet,
                service_account      = args.service_account,
                private_key          = decode_identity(args.service_account_private_key),
                slugify              = args.slugify,
                column_rename_map    = json.loads(args.col_rename) if args.col_rename else None
            )
        except json.decoder.JSONDecodeError as e:
            logger.error("Invalid JSON passed to --rename")
            raise

    elif args.service_account_file is not None or default_identity_file.exists():
        identity=(args.service_account_file if args.service_account_file else default_identity_file)

        if args.verbose>=2:
            encode_identity(identity, logger)

        try:
            tables = gsheetstables.GSheetsTables(
                gsheetid             = args.gsheet,
                service_account_file = identity,
                slugify              = args.slugify,
                column_rename_map    = json.loads(args.col_rename) if args.col_rename else None
            )
        except json.decoder.JSONDecodeError as e:
            logger.error("Invalid JSON passed to --rename")
            raise

    else:
        logger.error("Either pass an identity file with -i or pure identity with -c and -m. Aborting.")
        sys.exit(1)

    db = get_db(args.db_url, args.verbose>0)

    # 1. Run sql_pre script
    # 2. Check if spreadhseet time is more recent than table snapshot in DB
    # 3. Write data to auxiliary table
    # 4. Compare last official snapshot with new data on auxiliary table
    # 5. Append auxiliary table into target table with new timestamp
    # 6. Drop auxiliary table
    # 7. Cleanup old data from tables, in case of appending
    # 8. Run sql_post script

    timestamp_col='_gsheet_utc_timestamp'

    status = dotmap.DotMap(
        created     = [],
        updated     = [],
        unchanged   = [],
        old_purge   = [],
    )

    with db.begin() as db_connection:
        if args.sql_pre:
            script = jinja2.Template(args.sql_pre).render(tables=tables.tables)

            if args.sql_split_char and args.sql_split_char in script:
            	# Script has multiple commands
                script=[s for s in (s.strip() for s in script.split(args.sql_split_char)) if s]
            else:
            	# Script is only 1 command
            	script=[script]

            for s in script:
                ss=' '.join(s.split())
                logger.debug(f"Run pre ETL SQL command: {ss}")
                db_connection.execute(sqlalchemy.text(s))

        now = datetime.datetime.now(datetime.timezone.utc)
        textual_db_schema=f"{args.db_schema}." if args.db_schema else ''


        for table in tables.tables:
            logger.debug(f"DB table update logic for {table}...")

            final_table = f"{args.table_prefix}{table}"

            table_exists = (
                sqlalchemy.inspect(db_connection)
                .has_table(
                    table_name = final_table,
                    schema     = args.db_schema
                )
            )
            logger.debug(f"Check if target «{textual_db_schema}{final_table}» exists for {table}: {table_exists}")

            target_table=f'{final_table}___tmp_' if table_exists else final_table

            # Check if table in DB needs an update by comparing DB’s table
            # timestamps and spreadsheet last modification time.
            if tables.modification_time and table_exists:

                versions_query = (
                    sqlalchemy.text(
                        textwrap.dedent(f"""\
                            SELECT DISTINCT {timestamp_col}
                            FROM {textual_db_schema}{final_table}
                            WHERE {timestamp_col} >= :modification_time"""
                        )
                    )
                    .bindparams(modification_time=tables.modification_time.replace(microsecond=0))
                    .compile(
                        dialect=db.dialect,
                        compile_kwargs=dict(literal_binds=True)
                    )
                )

                logger.debug(f"Checking if {table} requires update with query: {versions_query}")

                versions = pandas.read_sql_query(versions_query, con=db_connection)
                if len(versions) > 0:
                    # DB already has data with timestamp equal or more
                    # recent than the spreadsheet last modification time.

                    status.unchanged.append(table)

                    logger.debug(f"Table {table} doesn‘t need update in DB.")

                    continue
                else:
                    logger.debug(f"Table {table} might have new data; row by row comparison triggered.")

            df = tables.t(table)

            # Work through a SQLAlchemy or PsycoPG or PostgreSQL INSERT limitation
            pages     = math.ceil((df.shape[0] * df.shape[1]) / 65000)
            page_size = math.floor(df.shape[0] / pages)+1

            if pages == 1:
                logger.debug(f"Write table data initially to {target_table}, all data at once.")
            else:
                logger.debug(f"Write table data initially to {target_table}, {pages} page(s) of {page_size} rows each.")

            # Write DataFrame to DB, either as a temporary table suited for
            # data comparison, or as the final table
            for i in range(0, len(df), page_size):
                logger.debug(f"{target_table}: chunk {i}:{page_size + i}")

                control_cols = [timestamp_col,gsheetstables.GSheetsTables.row_col]

                (
                    df

                    # Current page (relevant only on large tables)
                    .iloc[i:page_size+i]

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
                    [control_cols + [c for c in df.columns if c not in control_cols]]

                    # Write to database, finally
                    .to_sql(
                        target_table,
                        schema=args.db_schema,
                        if_exists='append',
                        method='multi',
                        con=db_connection,
                        index=False
                    )
                )

            # Check if data really changed
            if table_exists is True:
                col_compare = ' OR '.join([
                    f"current.{c} <> {target_table}.{c}"
                    for c in tables.t(table).columns
                    if c not in {gsheetstables.GSheetsTables.row_col}
                ])

                # If the following query returns more than zero lines, table
                # has changed and requires update.
                # Query is a bit too complex to keep compatibility with all DBs,
                # specially those that don’t support full outer join (MariaDB).
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

                diff = pandas.read_sql_query(diff_query, con=db_connection)
                if len(diff) > 0:
                    # Data of this scpecific table has changed, append to main
                    # table.

                    status.updated.append(table)
                    logger.debug(f"Detected change in data; updating {final_table}")

                    db_connection.execute(
                        sqlalchemy.text(textwrap.dedent(f"""\
                            INSERT INTO {textual_db_schema}{final_table}
                            SELECT * FROM {textual_db_schema}{target_table}
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
                    sql=textwrap.dedent(f"""
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
                        "Delete anything older than last allowed snapshot: ",
                        oldest
                    )

                    db_connection.execute(
                        sqlalchemy.text(textwrap.dedent(f"""\
                            DELETE FROM {textual_db_schema}{final_table}
                            WHERE {timestamp_col} <= :time
                            """
                        )),
                        dict(time = oldest)
                    )

        if args.sql_post:
            script = jinja2.Template(args.sql_post).render(tables=tables.tables)

            if args.sql_split_char and args.sql_split_char in script:
                script=[s for s in (s.strip() for s in script.split(args.sql_split_char)) if s]
            else:
                script=[script]

            for s in script:
                ss=' '.join(s.split())
                logger.debug(f"Run post ETL SQL command: {ss}")
                db_connection.execute(sqlalchemy.text(ss))

    db.dispose()

    # Display some status
    if len(status.created)>0:
        logger.warning("Tables created: 🧮"                  + ', 🧮'.join(status.created))
    if len(status.updated)>0:
        logger.warning("Tables updated: 🧮"                  + ', 🧮'.join(status.updated))
    if len(status.unchanged)>0:
        logger.info("Tables unchanged: 🧮"                   + ', 🧮'.join(status.unchanged))
    if len(status.old_purge)>0:
        logger.warning("Tables freed of old records: 🧮"     + ', 🧮'.join(status.old_purge))


if __name__ == "__main__":
    main()
