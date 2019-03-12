import multiprocessing as mp
import logging
from multiprocessing import util
import time
import signal
import os
from objects.injector import Injector


util.log_to_stderr(level=logging.WARN)

# Start methods
# windows and linux
# mp.set_start_method('spawn')
# linux only
# mp.set_start_method('spawn')
# Available on Unix platforms which support passing file descriptors over Unix pipes.
# mp.set_start_method('forkserver')


def redirect_load(arg):
    return redirect_load.func(arg)


def init_redirect_load(redirect_load, table):
    saver = Injector(table)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    redirect_load.func = saver.ingest_row


def run_in_parallel(producer, to_sink):
    mp.set_start_method('spawn')
    logging.info("CPU=%d" % mp.cpu_count())
    # TODO add n=cores as parameters
    pool = mp.Pool(None, init_redirect_load, [redirect_load, to_sink])
    start_time = time.time()
    try:
        res = pool.imap_unordered(redirect_load, producer.generate_row())
        pool.close()
        pool.join()
    except KeyboardInterrupt:
        print("Exception")
        pool.terminate()
        pool.join()
        producer.stop_generate_row()
    producer.stop_generate_row()
    took = time.time() - start_time
    logging.info("[%u] Total time %s" % (os.getpid(), took))


