import multiprocessing as mp


def worker(i, ds):
    # print(i)
    import time
    time.sleep(1)
    print(hex(id(ds)))
    return ds[i] * ds[i]


def main():
    ds = {k: k for k in range(10)}
    pool = mp.Pool(processes=mp.cpu_count())

    results = [pool.apply_async(worker, (i, ds)) for i in range(10)]
    pool.close()
    pool.join()
    print([r.get() for r in results])

    #results = [r.get() for r in [pool.apply_async(worker, (i, ds)) for i in range(10)]]
    #print(results)

if __name__ == '__main__':
    main()
