# -*- coding: utf-8 -*-
import csv
import json
import threading
import time
import traceback
from itertools import islice
from multiprocessing.pool import ThreadPool

import pymysql

TABLE_NAME = 'tamr'
DB_NAME = 'testdata'

POOL_SIZE = 32
conn_pool = []
conn_pool_lock = threading.Lock()


def get_conn():
    with conn_pool_lock:
        if len(conn_pool) == 0:
            return pymysql.connect(host='april-test.ctj5svng4bym.us-west-2.rds.amazonaws.com', port=3306,
                                   user='*******', passwd='******', db='testdata')
        return conn_pool.pop()


def release_conn(conn):
    if conn is None:
        return
    with conn_pool_lock:
        if len(conn_pool) < POOL_SIZE:
            conn_pool.append(conn)


def split_every(n, iterable):
    i = iter(iterable)
    piece = list(islice(i, n))
    while piece:
        yield piece
        piece = list(islice(i, n))


def run_sql_no_fetch(sql):
    conn = None
    cur = None
    err = None
    for retry in range(3):
        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute(sql)
            return conn.commit()
        except Exception as e:
            err = e
            time.sleep(0.2)
            stack_trace = traceback.format_exc().lower()
            if 'mysql' in stack_trace and 'connection' in stack_trace:
                if conn is not None:
                    try:
                        conn.close()
                    except:
                        pass
                    conn = None
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                release_conn(conn)
    raise err


failed_chunks_log = 'failed_inserting2'
failed_chunks_log_lock = threading.Lock()


def insert_many(rows):
    try:
        sql = """
        INSERT INTO tamr VALUES %s
        """ % (
            ", ".join(
                map(lambda row: "(%s)" % ", ".join('"%s"' % col.replace('"', r'\"') for col in row), rows)
            )
        )
        run_sql_no_fetch(sql)
    except Exception as e:
        failed_chunk = json.dumps(rows) + "\n"
        with failed_chunks_log_lock:
            with open(failed_chunks_log, 'a') as f:
                f.write(failed_chunk)
        print('error happened when inserting chunk: %s' % traceback.format_exc())
        return e


def truncate_table():
    sql = """
    TRUNCATE TABLE tamr
    """
    run_sql_no_fetch(sql)


def copy_from_csv_to_db(csv_file):
    pool = None
    try:
        pool = ThreadPool()
        with open(csv_file, 'r') as f:
            csv_reader = csv.reader(f)
            for macro_chunks in split_every(1000 * POOL_SIZE * 10, csv_reader):
                list(pool.imap_unordered(insert_many, split_every(1000, macro_chunks)))
        print("Done")
    finally:
        if pool is not None:
            pool.close()


if __name__ == '__main__':
    copy_from_csv_to_db('final.csv')
