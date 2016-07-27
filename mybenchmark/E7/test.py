import random

from elastic.util import statistics


def test():
    print('test....')
    dist = []
    for i in range(10000):
        #dist.append(random.paretovariate(3))
        dist.append(random.uniform(0, 1))

    r = statistics.inverse_transform_sampling(dist, n_samples=100)
    print(r)


if __name__ == "__main__":
    test()