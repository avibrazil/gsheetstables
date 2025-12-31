#!/usr/bin/env python

# Used only when testing locally
# import sys
# sys.path.insert(0,'..')

import sys
import os
import datetime
import textwrap
import argparse
import pathlib
import logging
import base64
import json

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
        '-s', '--sheet',
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
        '-p', '--table-prefix',
        dest='table_prefix',
        required=False,
        default='',
        help='Prefix this string to every table name in the target database'
    )

    parser.add_argument(
        '-i', '--identity-file',
        dest='service_account_file',
        required=False,
        default=None,
        help=f'Path to JSON file that contains the private key of account authorized to access the spreadsheet. Download it from Google Cloud Console. Defaults to {default_identity_file}'
    )

    parser.add_argument(
        '-c', '--service-account',
        dest='service_account',
        required=False,
        default=None,
        help='E-mail address of service account as created in Google Cloud Console'
    )

    parser.add_argument(
        '-m', '--service-account-private-key',
        dest='service_account_private_key',
        required=False,
        default=None,
        help='Encoded and encrypted private key. Run me with --identity-file and -vv to see what to pass.'
    )

    parser.add_argument(
        '-y', '--slugify',
        dest='slugify',
        action=argparse.BooleanOptionalAction,
        required=False,
        default=True,
        help='Slugify, simplify column names to be more database-friendly. Defaults to slugify.'
    )

    # parser.add_argument(
    #     '-r', '--row-numbers',
    #     dest='rows',
    #     action=argparse.BooleanOptionalAction,
    #     required=False,
    #     default=False,
    #     help='Write the spreadsheet row number as table column _GSheet_row. Defaults to not write.'
    # )

    # parser.add_argument(
    #     '-t', '--timestamp',
    #     dest='timestamp',
    #     action=argparse.BooleanOptionalAction,
    #     required=False,
    #     default=True,
    #     help='Write the UTC timestamp when this program runs as table column _GSheetsTables_utc_timestamp. Defaults to write timestamps.'
    # )

    parser.add_argument(
        '-a', '--append',
        dest='append',
        action=argparse.BooleanOptionalAction,
        required=False,
        default=False,
        help='Append data to existing table instead of droping and recreating table. Activates --timestamp too. Defaults to not append.'
    )

    parser.add_argument(
        '-n', '--keep-snapshots',
        dest='nsnapshots',
        type=int,
        required=False,
        default=3,
        help='Keep only the last N snapshots when using --append, and delete older ones. Pass 0 to never delete snapshots. Defaults to 3.'
    )

    parser.add_argument(
        '--sql-pre',
        dest='sql_pre',
        required=False,
        default=None,
        help='SQL script to execute before writing tables to DB. Can be a Jinja template. In case of multi-line script, use the char at --sql-split-char to separate each query.'
    )

    parser.add_argument(
        '--sql-post',
        dest='sql_post',
        required=False,
        default=None,
        help='SQL script to execute after writing tables to DB. Can be a Jinja template. In case of multi-line script, use the char at --sql-split-char to separate each query.'
    )

    parser.add_argument(
        '--sql-split-char',
        dest='sql_split_char',
        required=False,
        default=None,
        help='Character that separates single queries on multi-line pre and post SQL scripts. Tip: use unusual unicode chars as §, 𐩕, ꩜ etc'
    )

    parser.add_argument(
        '-v', '--verbose',
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
        tables = gsheetstables.GSheetsTables(
            gsheetid             = args.gsheet,
            service_account      = args.service_account,
            private_key          = decode_identity(args.service_account_private_key),
            slugify              = args.slugify,
        )
    elif args.service_account_file is not None or default_identity_file.exists():
        identity=(args.service_account_file if args.service_account_file else default_identity_file)

        if args.verbose>=2:
            encode_identity(identity, logger)

        tables = gsheetstables.GSheetsTables(
            gsheetid             = args.gsheet,
            service_account_file = identity,
            slugify              = args.slugify,
        )
    else:
        logger.error("Either pass an identity file with -i or pure identity with -c and -m. Aborting.")
        sys.exit(1)

    db = get_db(args.db_url, args.verbose>0)

    with db.begin() as db_connection:
        # 1. Run sql_pre script
        # 2. Check if spreadhseet time is more recent than table snapshot in DB
        # 3. Write data to auxiliary table
        # 4. Compare last official snapshot with new data on auxiliary table
        # 5. Append auxiliary table into target table with new timestamp
        # 6. Drop auxiliary table
        # 7. Cleanup old data from tables, in case of appending
        # 8. Run sql_post script

        if args.sql_pre:
            meta_script = jinja2.Template(args.sql_pre)
            script=meta_script.render(
                tables=tables.tables
            )

            logger.debug(f"Run pre SQL script: \n{script}")

            if args.sql_split_char:
                script=[s for s in (s.strip() for s in script.split(args.sql_split_char)) if s]

            logger.debug(f"Pre script: \n{script}")

            for s in script:
                db_connection.execute(sqlalchemy.text(s))

        now = datetime.datetime.now(datetime.timezone.utc)

        for table in tables.tables:

            # Check if table in DB needs an update by comparing DB’s table
            # timestamps and spreadsheet last modification time.
            if tables.modification_time:

                versions_query = (
                    sqlalchemy.text(
                        textwrap.dedent(f"""\
                            SELECT DISTINCT _GSheet_utc_timestamp
                            FROM {args.table_prefix}{table}
                            WHERE _GSheet_utc_timestamp >= :modification_time"""
                        )
                    )
                    .bindparams(modification_time=tables.modification_time.replace(microsecond=0))
                    .compile(
                        dialect=db.dialect,
                        compile_kwargs=dict(literal_binds=True)
                    )
                )

                logger.debug(f"Checking if {table} requires update with query: {versions_query}")

                try:
                    versions = pandas.read_sql_query(versions_query, con=db_connection)
                    if len(versions) > 0:
                        # DB already has data with timestamp equal or more
                        # recent than the spreadsheet last modification time.

                        logger.info(f"Table {table} doesn‘t need update in DB.")

                        continue
                    else:
                        logger.info(f"Table {table} will get new data in DB.")
                        table_exists=True

                except sqlalchemy.exc.ProgrammingError:
                    table_exists=False
                    logger.warning(f"Can’t check if table {table} requires a DB update; seems it doesn’t exist in database, so creating anyway. You should worry if you see this warning again and again in the future.")


            final_table = args.table_prefix + table
            target_table=f'__tmp_{final_table}' if table_exists else final_table

            logger.debug(f"Write table data initially to {target_table}")

            # Write DataFrame to DB, either as a temporary table suited for data
            # comparison, or as the final table
            (
                tables.t(table)

                .assign(_GSheet_utc_timestamp=tables.modification_time.replace(microsecond=0))

                .to_sql(
                    target_table,
                    if_exists=("append" if args.append else "replace"),
                    con=db_connection,
                    index=True
                )
            )

            # Check if data really changed
            if table_exists:

                logger.debug(f"Compare new data with last snapshot")

                col_compare = ' OR '.join([
                    f"current.`{c}` <> {target_table}.`{c}`"
                    for c in tables.t(table).columns
                    if c not in {'_GSheet_row'}
                ])

                # If the following query returns more than zero lines, table
                # has changed and requires update.
                # Query is a bit too complex to keep compatibility with all DBs,
                # specially those that don’t support full outer join (MariaDB).
                diff_query = textwrap.dedent(f"""\
                    WITH current AS (
                    	SELECT *
                    	FROM {final_table}
                    	WHERE _GSheet_utc_timestamp = (
                            SELECT MAX(_GSheet_utc_timestamp)
                            FROM {final_table}
                        )
                    ),
                    diff_left AS (
                    	SELECT current._GSheet_row
                    	FROM current
                    	LEFT JOIN {target_table}
                    	ON current._GSheet_row = {target_table}._GSheet_row
                    	WHERE {target_table}._GSheet_row is NULL OR {col_compare}
                    	LIMIT 1
                    ),
                    diff_right AS (
                    	SELECT {target_table}._GSheet_row
                    	FROM current
                    	RIGHT JOIN {target_table}
                    	ON current._GSheet_row = {target_table}._GSheet_row
                    	WHERE current._GSheet_row is NULL OR {col_compare}
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

                    logger.debug(f"Detected change in data; updating {final_table}")

                    db_connection.execute(
                        sqlalchemy.text(textwrap.dedent(f"""\
                            INSERT INTO {final_table}
                            SELECT * FROM {target_table}
                        """))
                    )
                else:
                    logger.debug(f"Data for table {final_table} didn't change; not updating")

                logger.debug(f"Drop auxiliary table {target_table}")
                db_connection.execute(
                    sqlalchemy.text(textwrap.dedent(f"""\
                        DROP TABLE {target_table}
                    """))
                )

            # Delete old table snapshots, keep only args.nsnapshots
            if args.append and args.nsnapshots>0:
                logger.debug(f"Delete old snapshots")
                db_connection.execute(sqlalchemy.text(textwrap.dedent(f"""\
                    DELETE t
                    FROM {final_table} AS t
                    LEFT JOIN (
                        SELECT _GSheet_utc_timestamp
                        FROM {final_table}
                        GROUP BY _GSheet_utc_timestamp
                        ORDER BY _GSheet_utc_timestamp DESC
                        LIMIT {args.nsnapshots}
                    ) AS keep
                    ON keep._GSheet_utc_timestamp = t._GSheet_utc_timestamp
                    WHERE keep._GSheet_utc_timestamp IS NULL
                    """))
                )


        if args.sql_post:
            meta_script = jinja2.Template(args.sql_post)
            script=meta_script.render(
                tables=tables.tables
            )

            logger.debug(f"Run post SQL script: \n{script}")

            if args.sql_split_char:
                script=[s for s in (s.strip() for s in script.split(args.sql_split_char)) if s]

            for s in script:
                db_connection.execute(sqlalchemy.text(s))



if __name__ == "__main__":
    main()
