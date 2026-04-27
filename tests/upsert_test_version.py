import os
import subprocess
import threading
import time
import random
import datetime
import sys
from clickhouse_driver import Client


class AtomicInteger:
    def __init__(self, initial=0):
        """初始化一个AtomicInteger对象"""
        self.value = initial
        self._lock = threading.Lock()

    def increment(self):
        """自增操作"""
        with self._lock:
            self.value += 1
            return self.value


count = AtomicInteger()


def increment_connter():
    counter.increment()
    return counter.get()


def process_data():
    client = None
    while client is None:
        try:
            client = Client('127.0.0.1', user='default', password='', database='default')
            query = 'SELECT LO_ORDERKEY, LO_CUSTKEY, LO_PARTKEY, LO_SUPPKEY, LO_ORDERDATE FROM lineorder_upsert_version ORDER BY rand() LIMIT 10000'
            r = client.execute(query)
        except Exception as e:
            print("Waiting for ClickHouse to start...")
            time.sleep(10)

    template = '''
               INSERT INTO lineorder_upsert_version (LO_ORDERKEY, \
                                                     LO_LINENUMBER, \
                                                     LO_CUSTKEY, \
                                                     LO_PARTKEY, \
                                                     LO_SUPPKEY, \
                                                     LO_ORDERDATE, \
                                                     LO_ORDERPRIORITY, \
                                                     LO_SHIPPRIORITY, \
                                                     LO_QUANTITY, \
                                                     LO_EXTENDEDPRICE, \
                                                     LO_ORDTOTALPRICE, \
                                                     LO_DISCOUNT, \
                                                     LO_REVENUE, \
                                                     LO_SUPPLYCOST, \
                                                     LO_TAX, \
                                                     LO_COMMITDATE, \
                                                     LO_SHIPMODE, VERSION) \
               VALUES ({}, 1, {}, {}, {}, '{}', 'C', 1, 2, 3, 4, 5, 6, 7, 8, '2015-07-07 18:45:00', 'C', {}), \
                      ({}, 1, {}, {}, {}, '{}', 'C', 1, 2, 3, 4, 5, 6, 7, 8, '2015-07-07 18:45:00', 'C', {}), \
                      ({}, 1, {}, {}, {}, '{}', 'C', 1, 2, 3, 4, 5, 6, 7, 8, '2015-07-07 18:45:00', 'C', {}), \
                      ({}, 1, {}, {}, {}, '{}', 'C', 1, 2, 3, 4, 5, 6, 7, 8, '2015-07-07 18:45:00', 'C', {}), \
                      ({}, 1, {}, {}, {}, '{}', 'C', 1, 2, 3, 4, 5, 6, 7, 8, '2015-07-07 18:45:00', 'C', {}), \
                      ({}, 1, {}, {}, {}, '{}', 'C', 1, 2, 3, 4, 5, 6, 7, 8, '2015-07-07 18:45:00', 'C', {}), \
                      ({}, 1, {}, {}, {}, '{}', 'C', 1, 2, 3, 4, 5, 6, 7, 8, '2015-07-07 18:45:00', 'C', {}), \
                      ({}, 1, {}, {}, {}, '{}', 'C', 1, 2, 3, 4, 5, 6, 7, 8, '2015-07-07 18:45:00', 'C', {}), \
                      ({}, 1, {}, {}, {}, '{}', 'C', 1, 2, 3, 4, 5, 6, 7, 8, '2015-07-07 18:45:00', 'C', {}), \
                      ({}, 1, {}, {}, {}, '{}', 'C', 1, 2, 3, 4, 5, 6, 7, 8, '2015-07-07 18:45:00', 'C', {}) \
               '''

    l = len(r) - 1
    for i in range(1, 10000):
        r0 = random.randint(0, l)
        r1 = random.randint(0, l)
        r2 = random.randint(0, l)
        r3 = random.randint(0, l)
        r4 = random.randint(9, l)
        r5 = random.randint(0, l)
        r6 = random.randint(0, l)
        r7 = random.randint(0, l)
        r8 = random.randint(0, l)
        r9 = random.randint(9, l)
        version = count.increment()

        query = template.format(r[r0][0], r[r0][1], r[r0][2], r[r0][3], r[r0][4], version, r[r1][0], r[r1][1], r[r1][2],
                                r[r1][3], r[r1][4], version, r[r2][0], r[r2][1], r[r2][2], r[r2][3], r[r2][4], version,
                                r[r3][0], r[r3][1], r[r3][2], r[r3][3], r[r3][4], version, r[r4][0], r[r4][1], r[r4][2],
                                r[r4][3], r[r4][4], version, r[r5][0], r[r5][1], r[r5][2], r[r5][3], r[r5][4], version,
                                r[r6][0], r[r6][1], r[r6][2], r[r6][3], r[r6][4], version, r[r7][0], r[r7][1], r[r7][2],
                                r[r7][3], r[r7][4], version, r[r8][0], r[r8][1], r[r8][2], r[r8][3], r[r8][4], version,
                                r[r9][0], r[r9][1], r[r9][2], r[r9][3], r[r9][4], version)
        try:
            client = Client('127.0.0.1', user='default', password='', database='default')
            client.execute(query)
        except Exception as e:
            print("Process data waiting for ClickHouse to start... error:{e}")
            time.sleep(10)


def process_data1():
    template = 'INSERT INTO lineorder_upsert_version SELECT * FROM lineorder_upsert_version LIMIT {} settings max_insert_threads=4'
    while True:
        try:
            client = Client('127.0.0.1', user='default', password='', database='default')
            client.execute("set max_memory_usage=100000000000")
            r = random.randint(100, 10000000)
            query = template.format(r)
            print(datetime.datetime.now().strftime("%Y%m%d%H%M%S") + " " + query)
            client.execute(query)
        except Exception as e:
            print(f"process_data1 Waiting for ClickHouse to start... error:{e}")
            time.sleep(10)


def mutation():
    while True:
        try:
            client = Client('127.0.0.1', user='default', password='', database='default')
            modify_column_query_uint64 = 'ALTER TABLE lineorder_upsert_version MODIFY COLUMN `LO_SUPPLYCOST` UInt64 SETTINGS mutations_sync = 2;'
            modify_column_query_uint32 = 'ALTER TABLE lineorder_upsert_version MODIFY COLUMN `LO_SUPPLYCOST` UInt32 SETTINGS mutations_sync = 2;'
            modify_query_list = []

            modify_query_list.append(modify_column_query_uint64)
            modify_query_list.append(modify_column_query_uint32)
            query = random.choice(modify_query_list)
            print("Mutation query:", query)
            client.execute(query)
            time.sleep(1)
        except Exception as e:
            print("Waiting for ClickHouse to start... mutation error:", e)
            time.sleep(30)


def checking():
    while True:
        try:
            client = Client('127.0.0.1', user='default', password='', database='default')
            query = 'SELECT count() FROM lineorder_upsert_version'
            r = client.execute(query)
            if r[0][0] != 59986052:
                print(datetime.datetime.now().strftime("%Y%m%d%H%M%S") + "======================MISMATCH!!!!!:" + str(
                    r[0][0]))
            else:
                print(datetime.datetime.now().strftime("%Y%m%d%H%M%S") + "======================OK!")
            time.sleep(5)
        except Exception as e:
            print(f"checking Waiting for ClickHouse to start... error:{e}")
            time.sleep(5)


def find_clickhouse_process():
    ps_output = subprocess.check_output(['ps', '-ef']).decode('utf-8')
    for line in ps_output.splitlines():
        if '/usr/bin/clickhouse-server --config' in line:
            return int(line.split()[1])
    return None


def restart_clickhouse_process():
    while True:
        try:
            sleep_time = random.randint(90, 180)
            print("Try to restart ClickHouse process")
            time.sleep(sleep_time)
            pid = find_clickhouse_process()
            if pid:
                os.kill(pid, 9)
                print(f"Killed ClickHouse process with PID {pid}")
            time.sleep(10)
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            log_file = open(f'logs/clickhouse_{timestamp}.log', 'w')
            subprocess.Popen(['service', 'clickhouse-server', 'start'], stdout=log_file, stderr=subprocess.STDOUT)
            print("Started ClickHouse process")
            time.sleep(120)
        except Exception as e:
            print(f"restart_clickhouse_process Waiting for ClickHouse to start... error:{e}")
            time.sleep(30)


def main():
    print("=====step-2: concurrent insert select ======")
    number_of_threads = 2
    threads = []

    for i in range(number_of_threads):
        thread = threading.Thread(target=process_data1)
        threads.append(thread)
        thread.start()

    print("=====step-3: concurrent insert duplicated ======")
    for i in range(number_of_threads):
        thread = threading.Thread(target=process_data)
        threads.append(thread)
        thread.start()

    print("=====step-4: check number ======")
    for i in range(2):
        checking_thread = threading.Thread(target=checking)
        checking_thread.start()
        threads.append(checking_thread)

    print("=====step-5: restart ======")
    restart_thread = threading.Thread(target=restart_clickhouse_process)
    restart_thread.start()
    threads.append(restart_thread)

    print("=====step-5: mutation ======")
    for i in range(1):
        mutation_thread = threading.Thread(target=mutation)
        mutation_thread.start()
        threads.append(mutation_thread)

    print("=====step-6: join threads======")
    for thread in threads:
        thread.join()


if __name__ == '__main__':
    main()
