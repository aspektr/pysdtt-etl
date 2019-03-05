import multiprocessing as mp
import logging
from multiprocessing import util
import time
import os
import signal
from objects.injector import Injector


util.log_to_stderr(level=logging.DEBUG)

# Start methods
# windows and linux
# mp.set_start_method('spawn')
# linux only
# mp.set_start_method('spawn')
# Available on Unix platforms which support passing file descriptors over Unix pipes.
# mp.set_start_method('forkserver')


def pprint(x):
    print(os.getpid(), x[2])


def redirect_load(arg):
    return redirect_load.func(arg)


def init_redirect_load(redirect_load, table):
    saver = Injector(table)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    redirect_load.func = saver.ingest_row


def run_in_parallel(producer):
    mp.set_start_method('spawn')
    logging.info("CPU=%d" % mp.cpu_count())
    # TODO add n=cores as parameters
    # TODO Replace hard-code name of table. Get it from command-promt
    pool = mp.Pool(None, init_redirect_load, [redirect_load, 'application_history'])  # 'temp_table'
    start_time = time.time()
    try:
        res = pool.imap_unordered(redirect_load, producer.generate_row())
        pool.close()
        pool.join()
        #_ = pool.map(redirect_load, producer.generate_row())
    except KeyboardInterrupt:
        print("Exception")
        pool.terminate()
        pool.join()
        producer.stop_generate_row()
    # producer.stop_generate_row()
    took = time.time() - start_time
    logging.info("Total time %s" % took)

