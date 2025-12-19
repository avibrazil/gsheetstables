#!/usr/bin/env python

# Used only when testing locally
# import sys
# sys.path.insert(0,'..')

import datetime
import textwrap
import argparse
import pathlib
import logging
import sqlalchemy
import jinja2
import gsheetstables


def prepare_logging(verbose: int):
    level = logging.WARNING
    if verbose == 1:
        level = logging.INFO
    elif verbose >= 2:
        level = logging.DEBUG

    logging.basicConfig(
        level=logging.INFO,  # default level
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
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
        '-i', '--identity_file',
        dest='service_account_file',
        required=False,
        default=pathlib.Path.home() / 'service_account.json',
        help='Path to JSON file that contains the private key of account authorized to access the spreadsheet. Download it from Google Cloud Console.'
    )

    parser.add_argument(
        '-y', '--slugify',
        dest='slugify',
        action=argparse.BooleanOptionalAction,
        required=False,
        default=True,
        help='Slugify, simplify column names to be more database-friendly. Defaults to slugify.'
    )

    parser.add_argument(
        '-r', '--row-numbers',
        dest='rows',
        action=argparse.BooleanOptionalAction,
        required=False,
        default=False,
        help='Write the spreadsheet row number as table column _GSheet_row. Defaults to not write.'
    )

    parser.add_argument(
        '-t', '--timestamp',
        dest='timestamp',
        action=argparse.BooleanOptionalAction,
        required=False,
        default=False,
        help='Write the UTC timestamp when this program runs as table column _GSheetsTables_utc_timestamp. Defaults to not write.'
    )

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
def get_db(db_url):
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
            # echo              = True
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



def main():
    # Read environment and command line parameters
    args=prepare_args()

    if args.append:
        args.timestamp=True

    # Setup logging
    global logger
    logger=prepare_logging(args.verbose)

    tables = gsheetstables.GSheetsTables(
        gsheetid             = args.gsheet,
        service_account_file = args.service_account_file,
        slugify              = args.slugify,
    )

    db = get_db(args.db_url)

    with db.begin() as db_connection:
        # 1. Run sql_pre script
        # 2. Write data to DB
        # 3. Cleanup old data from tables, in case of appending
        # 4. Run sql_post script

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
            (
                tables.t(table)

                .pipe(
                    lambda table: (
                        table.assign(_GSheetsTables_utc_timestamp=now)
                        if args.timestamp
                        else table
                    )
                )

                .to_sql(
                    table,
                    if_exists=("append" if args.append else "replace"),
                    con=db_connection,
                    index=args.rows
                )
            )

            if args.append and args.nsnapshots>0:
                db_connection.execute(sqlalchemy.text(textwrap.dedent(f"""\
                    DELETE
                    FROM {table}
                    WHERE _GSheetsTables_utc_timestamp NOT IN (
                        SELECT DISTINCT _GSheetsTables_utc_timestamp
                        FROM {table}
                        ORDER BY _GSheetsTables_utc_timestamp DESC limit {args.nsnapshots}
                    )"""))
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
