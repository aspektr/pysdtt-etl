import multiprocessing as mp
import logging
from multiprocessing import util
import time
import signal
import os
from objects.injector import Injector
import traceback
import functools
from functools import partial

util.log_to_stderr(level=logging.INFO)

# Start methods
# windows and linux
# mp.set_start_method('spawn')
# linux only
# mp.set_start_method('spawn')
# Available on Unix platforms which support passing file descriptors over Unix pipes.
# mp.set_start_method('forkserver')


def trace_unhandled_exceptions(func):
    @functools.wraps(func)
    def wrapped_func(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except:
            print('Exception in '+func.__name__)
            args[2].put(1)
            traceback.print_exc()
    return wrapped_func


def redirect_load(arg):
    return redirect_load.func(arg)


@trace_unhandled_exceptions
def init_redirect_load(redirect_load, table, q):
    saver = Injector(table)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    redirect_load.func = partial(saver.ingest_row, q)


def run_in_parallel(producer, to_sink):
    mp.set_start_method('spawn')
    queue = mp.Queue()
    logging.info("CPU=%d" % mp.cpu_count())
    # TODO add n=cores as parameters
    pool = mp.Pool(None, init_redirect_load, [redirect_load, to_sink, queue])
    start_time = time.time()
    try:
        pool.imap_unordered(redirect_load, producer.generate_row())
        pool.close()
        pool.join()

        if not queue.empty():
            producer.stop_generate_row()
            pool.terminate()
            raise SystemExit(1)
    # TODO exceptions don't work See http://jessenoller.com/blog/2009/01/08/multiprocessingpool-and-keyboardinterrupt
    except KeyboardInterrupt:
        pool.terminate()
        producer.stop_generate_row()
    producer.stop_generate_row()
    took = time.time() - start_time
    logging.info("[%u] Total time %s" % (os.getpid(), took))

