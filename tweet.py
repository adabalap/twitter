import argparse
import json
import logging
import sqlite3
from sqlite3 import Error

from ShiningArmor import twitter

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)


def get_cmd_line_args():
    """
    parse the command line arguments
    """
    ap = argparse.ArgumentParser()

    ap.add_argument("-t", "--tokens_file", required=True)
    ap.add_argument("-d", "--db_file", required=True)
    ap.add_argument("-s", "--sql_file", required=True)

    args = vars(ap.parse_args())

    logging.info(f'Tokens file is - {args["tokens_file"]}')
    logging.info(f'Sqlite3 DB file is - {args["db_file"]}')
    logging.info(f'SQL file is - {args["sql_file"]}')

    return args


def db_conn(db_file):
    # Get DB connection
    try:
        db = sqlite3.connect(db_file)
    except Error as err:
        raise err

    return db


def db_query(db, sql_stmt):
    # Execute the SQL statement
    try:
        cur = db.cursor()
        cur.execute(sql_stmt['select'])
        rec = cur.fetchone()
    except Error as err:
        raise err

    return rec


def db_sql(sql_file):
    # Get SQL statements - SELECT/UPDATE
    try:
        f = open(sql_file, mode='r')
        sql_stmt = json.load(f)
        f.close()
    except FileNotFoundError as err:
        raise err
    except IOError as err:
        raise err

    return sql_stmt


if __name__ == "__main__":
    args = get_cmd_line_args()

    # Get DB connection
    db = db_conn(args['db_file'])

    # Get SQL statements
    sql_stmt = db_sql(args['sql_file'])

    # Prepare & Execute SQL
    rec = db_query(db, sql_stmt)

    try:
        tokens = twitter.tokens(args['tokens_file'])
        api = twitter.auth(tokens)
        twitter.tweet(api, rec['msg'])
    except Error as err:
        logging.error(err)
        raise err
