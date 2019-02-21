import multiprocessing as mp
import logging
from multiprocessing import util

util.log_to_stderr(level=logging.DEBUG)

# Start methods
# windows and linux
# mp.set_start_method('spawn')
# linux only
# mp.set_start_method('spawn')
# Available on Unix platforms which support passing file descriptors over Unix pipes.
# mp.set_start_method('forkserver')


def my_func(x):
    print(mp.current_process())
    job = x**x
    print(job)
    return job


def main():
    mp.set_start_method('spawn')
    print("CPU=%d" % mp.cpu_count())
    pool = mp.Pool(mp.cpu_count())
    result = pool.map(my_func, [50, 100])
    #result_set_2 = pool.map(my_func, range(200))


if __name__ == "__main__":
    main()