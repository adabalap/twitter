import argparse

from ShiningArmor import twitter

import sqlite3
from sqlite3 import Error

import logging


logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',level=logging.DEBUG)


def get_cmd_line_args():
    """
    parse the command line arguments
    """
    ap = argparse.ArgumentParser()

    ap.add_argument("-t", "--tokens_file", required=True)
    ap.add_argument("-d", "--db_file", required=True)
    ap.add_argument("-s", "--sql_file", required=True)

    args = vars(ap.parse_args())

    logging.info(f'Tokens file is - {args['tokens_file']}')
    logging.info(f'Sqlite3 DB file is - {args['db_file']}')
    logging.info(f'SQL file is - {args['sql_file']}')

    return args


def get_sql(args):
    try:
        fh = open(args['sql_file'], "r")
        args['sql'] = fh.read()
    except Error as err:
        logging.error(err)
        args['rc'] = 1

    return args


def get_msg(args):

    # Get the SQL statement
    args = get_sql(args['sql_file'])

    if args['rc'] == 1:
       return args

    try:
        # Create a database connection
        args['db_conn'] = None
        args['db_conn'] = sqlite3.connect(args['db_file'])

        # Query the table
        cur = args['db_conn'].cursor()
        cur.execute(args['sql'])
        args['record'] = cur.fetchone()
    except Error as err:
        logging.error(err)
        args['rc'] = 1

    return args


if __name__ == "__main__":
    args = get_cmd_line_args()
    args['rc'] = 0

    #Get tweet messge from database
    args = get_msg(args)

    if args['rc'] == 0:
        try:
            tokens = twitter.tokens(args['tokens_file'])
            api = twitter.auth(tokens)
            twitter.tweet(api, msg)
        except Error as err:
            logging.error(err)
            args['rc'] = 1
    else:
        logging.error(f'Unable to get messages to tweet')

    return args['rc']

