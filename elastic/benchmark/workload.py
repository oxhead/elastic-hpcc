import bisect
import copy
from enum import Enum
import random

import yaml

from elastic.benchmark import config
from elastic.util import helper


class WorkloadConfig(config.BaseConfig):
    pass


class WorkloadType(Enum):
    poisson = 0
    constant = 1
    onetime = 2


class DistributionType(Enum):
    fixed = 0
    uniform = 1
    gaussian = 2
    exponential = 3
    long_normal = 4
    beta = 5
    gamma = 6
    pareto = 7


class WorkloadGenerator:
    def next(self):
        pass


class ConstantWorkloadGenerator(WorkloadGenerator):
    def __init__(self, volume):
        self.volume = volume

    def next(self):
        return self.volume


class UniformWorkloadGenerator(WorkloadGenerator):
    def __init__(self, mu, sigma):
        self.mu = mu
        self.sigma = sigma

    def next(self):
        return int(random.uniform(self.mu, self.sigma))


class ExponentialWorkloadGenerator(WorkloadGenerator):
    def __init__(self, lambd):
        self.lambd = lambd

    def next(self):
        return int(random.expovariate(self.lambd))


class Workload:

    def __init__(self, workload_type, workload_generator, application_selection, period=None):
        self.workload_type = workload_type
        self.workload_generator = workload_generator
        self.application_selection = application_selection
        self.period = period
        self.taken_count = 0

    def next(self):
        if self.period is None:
            return self.workload_generator.next()
        else:
            if self.taken_count >= self.period:
                return None
            self.taken_count += 1
            return self.workload_generator.next()

    @staticmethod
    def new_generator(workload_type, num_queries):
        if workload_type is WorkloadType.onetime:
            return ConstantWorkloadGenerator(num_queries)
        elif workload_type is WorkloadType.constant:
            return ConstantWorkloadGenerator(num_queries)
        elif workload_type is WorkloadType.poisson:
            return ExponentialWorkloadGenerator(1.0 / num_queries)
        raise Exception("unknown workload type: {}".format(workload_type))

    @staticmethod
    def from_config(config_path):
        workload_config = WorkloadConfig.parse_file(config_path)

        global_distribution = workload_config.lookup_config("workload.distribution")
        # print("@ global:", global_distribution)

        # roxie application setting
        apps = {}
        for (app_name, app_setting) in workload_config.lookup_config("workload.applications").items():
            endpoint = app_setting['endpoint']
            query_name = app_setting['query_name']
            query_key = app_setting['query_key']
            key_list = app_setting['key_list'] if 'key_list' in app_setting else helper.parse_file(app_setting['key_file'])
            selection_model = None
            if 'distribution' in app_setting:
                selection_model = SelectionModel.new(app_setting['distribution'], key_list)
            else:
                selection_model = SelectionModel.new(global_distribution, key_list)
            app = RoxieApplication(query_name, endpoint, query_key, selection_model)
            apps[app_name] = app

        # workload setting
        application_selection = SelectionModel.new(workload_config.lookup_config("workload.selection"), apps)
        workload_type = WorkloadType[workload_config.lookup_config('workload.type').lower()]
        num_queries = workload_config.lookup_config('workload.num_queries')
        period = workload_config.lookup_config("workload.period")
        workload_generator = Workload.new_generator(workload_type, num_queries)
        return Workload(workload_type, workload_generator, application_selection, period=period)


class SelectionModel:

    def __init__(self, distribution_type, probability_list, objects):
        self.distribution = distribution_type
        self.probability_list = probability_list
        self.objects = objects
        # TODO: not efficient?
        self.key_list = list(self.objects.keys()) if type(self.objects) is dict else list(range(len(objects)))

    def select(self):
        return self.objects[self.key_list[self.select_index()]]

    def select_index(self):
        return bisect.bisect_left(self.probability_list, random.random())

    @staticmethod
    def _produce_probability_distribution(rv):
        # probability
        rv_sum = sum(rv)
        rv_p = [_ / rv_sum for _ in rv]
        # accumulated probability
        rv_p_accumulate = copy.copy(rv_p)
        for i in range(1, len(rv_p_accumulate)):
            rv_p_accumulate[i] = rv_p_accumulate[i - 1] + rv_p_accumulate[i]
        return rv_p_accumulate

    @staticmethod
    def new(config_object, objects):
        # print("@", config_object['type'].lower())
        distribution_type = DistributionType[config_object['type'].lower()]
        if distribution_type == DistributionType.fixed:
            return SelectionModel.new_fixed(objects, config_object['percentage'])
        elif distribution_type == DistributionType.uniform:
            return SelectionModel.new_uniform(objects)
        elif distribution_type == DistributionType.gaussian:
            return SelectionModel.new_gauss(objects, config_object['mu'], config_object['sigma'])
        elif distribution_type == DistributionType.exponential:
            return SelectionModel.new_exponential(objects, config_object['lambda'])
        elif distribution_type == DistributionType.long_normal:
            return SelectionModel.new_longnormal(objects, config_object['mu'], config_object['sigma'])
        elif distribution_type == DistributionType.beta:
            return SelectionModel.new_beta(objects, config_object['alpha'], config_object['beta'])
        elif distribution_type == DistributionType.gamma:
            return SelectionModel.new_gamma(objects, config_object['alpha'], config_object['beta'])
        elif distribution_type == DistributionType.pareto:
            return SelectionModel.new_pareto(objects, config_object['alpha'])
        raise Exception("Undefined distribution type")

    @staticmethod
    def new_fixed(objects, percentages):
        if type(objects) is not type(percentages):
            raise Exception("the parameters need to have the same type: either dict or list")
        if type(objects) is list:
            # not sure about the data type here
            probability_list = SelectionModel._produce_probability_distribution([float(p) for p in percentages])
            return SelectionModel(DistributionType.fixed, probability_list, objects)
        elif type(objects) is dict:
            object_list = []
            probability_list = []
            for key in objects.keys():
                object_list.append(objects[key])
                probability_list.append(float(percentages[key]))
            probability_list = SelectionModel._produce_probability_distribution(probability_list)
            return SelectionModel(DistributionType.fixed, probability_list, object_list)

    @staticmethod
    def new_uniform(objects):
        # random variables
        rv = [random.uniform(0, 1) for _ in range(len(objects))]
        probability_list = SelectionModel._produce_probability_distribution(rv)
        return SelectionModel(DistributionType.uniform, probability_list, objects)

    @staticmethod
    def new_gauss(objects, mu=1, sigma=0.2):
        # random variables
        rv = [random.gauss(mu, sigma) for _ in range(len(objects))]
        probability_list = SelectionModel._produce_probability_distribution(rv)
        return SelectionModel(DistributionType.gaussian, probability_list, objects)

    @staticmethod
    def new_exponential(objects, lambd=1):
        # random variables
        rv = [random.expovariate(lambd) for _ in range(len(objects))]
        probability_list = SelectionModel._produce_probability_distribution(rv)
        return SelectionModel(DistributionType.exponential, probability_list, objects)

    @staticmethod
    def new_longnormal(objects, mu=0, sigma=1):
        # random variables
        rv = [random.lognormvariate(mu, sigma) for _ in range(len(objects))]
        probability_list = SelectionModel._produce_probability_distribution(rv)
        return SelectionModel(DistributionType.longnormal, probability_list, objects)

    @staticmethod
    def new_beta(objects, alpha=2, beta=5):
        # random variables
        rv = [random.betavariate(alpha, beta) for _ in range(len(objects))]
        probability_list = SelectionModel._produce_probability_distribution(rv)
        return SelectionModel(DistributionType.beta, probability_list, objects)

    @staticmethod
    def new_gamma(objects, alpha=9., beta=.5):
        # random variables
        rv = [random.gammavariate(alpha, beta) for _ in range(len(objects))]
        probability_list = SelectionModel._produce_probability_distribution(rv)
        return SelectionModel(DistributionType.gamma, probability_list, objects)

    @staticmethod
    def new_pareto(objects, alpha=3):
        # random variables
        rv = [random.paretovariate(alpha) for _ in range(len(objects))]
        probability_list = SelectionModel._produce_probability_distribution(rv)
        return SelectionModel(DistributionType.pareto, probability_list, objects)


class RoxieApplication:
    def __init__(self, query_name, endpoint, query_key, selection_model):
        self.query_name = query_name
        self.endpoint = endpoint
        self.query_key = query_key
        self.selection_model = selection_model

    def next_query(self):
        return self.query_name, self.endpoint, self.query_key, self.selection_model.select()
