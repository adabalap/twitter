import argparse
import json
import logging
import sqlite3
from ShiningArmor import twitter
from sqlite3 import Error

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)


def get_cmd_line_args():
    """
    parse the command line arguments
    """
    ap = argparse.ArgumentParser()

    ap.add_argument("-t", "--tokens_file", required=True)
    ap.add_argument("-d", "--db_file", required=True)
    ap.add_argument("-s", "--sql_file", required=True)
    ap.add_argument("-ht", "--hash_tag", required=True)

    args = vars(ap.parse_args())

    logging.info(f'Tokens file: {args["tokens_file"]}')
    logging.info(f'Sqlite3 DB file: {args["db_file"]}')
    logging.info(f'SQL file: {args["sql_file"]}')
    logging.info(f'HASH Tag: {args["hash_tag"]}')

    return args


def db_conn(db_file):
    # Get DB connection
    try:
        db = sqlite3.connect(db_file)
    except Error as err:
        raise err

    return db


def db_query(db, sql_stmt, hash_tag):
    # Execute the SQL statement
    try:
        cur = db.cursor()
        # Parse the SQL and replace with dynamic value
        sql_stmt['select'] = str(sql_stmt['select']).replace('ZZZ', f'{hash_tag}')
        logging.debug(sql_stmt['select'])

        cur.execute(sql_stmt['select'])
        rec = cur.fetchone()
    except Error as err:
        raise err

    return rec


def db_update(db, sql_stmt):
    # Update the SQL statement
    try:
        cur = db.cursor()
        cur.execute(sql_stmt['update'])
        db.commit()
    except Error as err:
        raise err


def db_close_conn(db):
    # Close the DB connection
    db.close()


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
    # Parse command line arguments
    args = get_cmd_line_args()

    # Get DB connection
    db = db_conn(args['db_file'])

    # Get SQL statements
    sql_stmt = db_sql(args['sql_file'])

    # Prepare & Execute SQL
    rec = db_query(db, sql_stmt, args['hash_tag'])
    (rowid, msg) = (rec[0], rec[1])

    # Append the hash tag to the tweet message
    msg = f'{msg} \n\n#{args["hash_tag"]}'

    try:
        tokens = twitter.tokens(args['tokens_file'])
        api = twitter.auth(tokens)
        twitter.tweet(api, msg)

        logging.debug(f'Message: {msg}')
        logging.info('Successfully sent the TWEET')
    except Error as err:
        logging.error(err)
        raise err

    # Update DB:
    #   - parse the SQL and replace with dynamic value
    sql_stmt['update'] = str(sql_stmt['update']).replace('ZZZ', f'{rowid}')
    logging.debug(sql_stmt['update'])

    db_update(db, sql_stmt)

    # Close DB connection
    db_close_conn(db)
