import bisect
import copy
from enum import Enum
import random
import logging
import pickle
import yaml
import hashlib

import numpy as np
import scipy.interpolate as interpolate
from scipy import stats


from elastic.benchmark import config
from elastic.util import helper
from elastic.util import collection as collection_helper


class WorkloadConfig(config.BaseConfig):
    @staticmethod
    def parse_file(config_path):
        with open(config_path, 'r') as f:
            return WorkloadConfig(yaml.load(f))

    def merge(self, added):
        '''
        This is a workaround.
        :param added: another config to be merged
        :return:
        '''
        self.set_config('workload.type', added.lookup_config('workload.type', self['workload.type']))
        self.set_config('workload.num_queries', added.lookup_config('workload.num_queries', self['workload.num_queries']))
        self.set_config('workload.period', added.lookup_config('workload.period', self['workload.period']))
        self.set_config('workload.dispatch_mode', added.lookup_config('workload.dispatch_mode', self['workload.dispatch_mode']))
        self.set_config('workload.applications', added.lookup_config('workload.applications', self['workload.applications']))
        self.set_config('workload.distribution', added.lookup_config('workload.distribution', self['workload.distribution']))
        self.set_config('workload.selection', added.lookup_config('workload.selection', self['workload.selection']))
        self.set_config('workload.endpoints', added.lookup_config('workload.endpoints', self['workload.endpoints']))

    def select_endpoints(self, num):
        self.set_config('workload.endpoints', self.lookup_config('workload.endpoints')[:num])

class WorkloadType(Enum):
    poisson = 0
    constant = 1
    onetime = 2


class DistributionType(Enum):
    fixed = 0
    uniform = 1
    normal = 2
    exponential = 3
    lognormal = 4
    beta = 5
    gamma = 6
    pareto = 7
    zipf = 8
    powerlaw = 9


class WorkloadGenerator:
    def next(self):
        pass

    def __repr__(self):
        return str(self.__class__.__name__)

    def __eq__(self, other):
        return self.__repr__() == other.__repr__()

    def __hash__(self):
        return helper.md5hash(self.__repr__())


class ConstantWorkloadGenerator(WorkloadGenerator):
    def __init__(self, volume):
        self.volume = volume

    def next(self):
        return self.volume

    def __repr__(self):
        return "".join([str(self.__class__.__name__), str(self.volume)])


class UniformWorkloadGenerator(WorkloadGenerator):
    def __init__(self, mu, sigma):
        self.mu = mu
        self.sigma = sigma

    def next(self):
        return int(random.uniform(self.mu, self.sigma))

    def __repr__(self):
        return "".join([str(self.__class__.__name__), str(self.mu), str(self.sigma)])


class ExponentialWorkloadGenerator(WorkloadGenerator):
    def __init__(self, lambd):
        self.lambd = lambd

    def next(self):
        return int(random.expovariate(self.lambd))

    def __key(self):
        return "".join([str(self.__class__.__name__), str(self.lambd)])


class RoxieTargetDispatcher:
    def __init__(self, target_list):
        self.target_list = target_list
        self.num_choices = len(self.target_list)
        self.current = 0

    def next(self):
        # will overflow?
        self.current += 1
        return self.target_list[self.current%self.num_choices]


class Workload:

    def __init__(self, workload_type, workload_generator, application_selection, period=None, dispatch_mode='batch'):
        self.workload_type = workload_type
        self.workload_generator = workload_generator
        self.application_selection = application_selection
        self.period = period
        self.taken_count = 0
        self.dispatch_mode = dispatch_mode

    def init(self):
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
    def parse_config(config_path):
        return Workload.from_config(WorkloadConfig.parse_file(config_path))

    @staticmethod
    def from_config(workload_config):
        # need to check before load config
        global_distribution = workload_config.lookup_config("workload.distribution")
        #print("@@", global_distribution)
        global_key_list = global_distribution['key_list'] if 'key_list' in global_distribution else helper.parse_file(global_distribution['key_file'])
        global_selection_model = SelectionModel.new(global_distribution, global_key_list)
        # print("@ global:", global_distribution)

        endpoint_list = []
        try:
            endpoint_list = workload_config.lookup_config("workload.endpoints")
        except:
            pass

        # roxie application setting
        apps = {}
        for (app_name, app_setting) in workload_config.lookup_config("workload.applications").items():
            #print(app_name, app_setting)
            #if 'endpoint' in app_setting:
            #    endpoint = app_setting['endpoint']
            query_name = app_setting['query_name']
            query_key = app_setting['query_key']
            if 'distribution' in app_setting:
                key_list = app_setting['key_list'] if 'key_list' in app_setting else helper.parse_file(app_setting['key_file'])
                selection_model = SelectionModel.new(app_setting['distribution'], key_list)

            else:
                # test whether the keys are sorted??
                #print(type(key_list), len(key_list))
                #chunk_size = int(len(key_list)/4)
                #print(len(key_list), chunk_size)
                #selection_model = SelectionModel.new(global_distribution, key_list[3*chunk_size:])
                #selection_model = SelectionModel.new(global_distribution, key_list)
                selection_model = global_selection_model
            # lazy hack here
            if query_name.endswith(']'):
                app_prefix = query_name.split('[')[0]
                #print("@@", app_prefix)
                range_str = query_name.split('[')[-1].split(']')[0]
                id_start, id_end = range_str.split('-')
                #print("##", id_start, id_end)
                for i in range(int(id_start), int(id_end) + 1):
                    new_query_name = "{}{}".format(app_prefix, i)
                    app = RoxieApplication(new_query_name, endpoint_list, query_key, selection_model)
                    apps[new_query_name] = app
            else:
                app = RoxieApplication(query_name, endpoint_list, query_key, selection_model)
                apps[app_name] = app

        # workload setting
        application_selection = SelectionModel.new(workload_config.lookup_config("workload.selection"), apps)
        workload_type = WorkloadType[workload_config.lookup_config('workload.type').lower()]
        num_queries = workload_config.lookup_config('workload.num_queries')
        period = workload_config.lookup_config("workload.period")
        dispatch_mode = workload_config.lookup_config("workload.dispatch_mode")
        workload_generator = Workload.new_generator(workload_type, num_queries)
        return Workload(workload_type, workload_generator, application_selection, period=period, dispatch_mode=dispatch_mode)

    # http://stackoverflow.com/questions/2909106/python-whats-a-correct-and-good-way-to-implement-hash
    def __repr__(self):
        apps_repr = ""
        for app_name in sorted(self.application_selection.objects.keys()):
            app = self.application_selection.objects[app_name]
            apps_repr += "[app={}".format(repr(app))
        return "".join([str(self.workload_type), "[generator=", repr(self.workload_generator), repr(self.application_selection.distribution), "[apps=", apps_repr, str(self.period)])

    def __eq__(self, other):
        return self.__repr__() == other.__repr__()

    def __hash__(self):
        return helper.md5hash(self.__repr__())


class SelectionModel:

    def __init__(self, distribution_type, probability_list, objects, **kwargs):
        self.distribution = distribution_type
        self.probability_list = probability_list
        self.objects = objects
        # TODO: not efficient?
        self.key_list = list(self.objects.keys()) if type(self.objects) is dict else list(range(len(objects)))
        self.kwargs = kwargs

    def select(self):
        return self.objects[self.key_list[self.select_index()]]

    def select_index(self):
        return bisect.bisect_left(self.probability_list, random.random()) - 1

    @staticmethod
    def _produce_probability_distribution(rv_list, bins):
        hist, bin_edges = np.histogram(rv_list, bins=bins, density=True)
        cum_values = np.zeros(bin_edges.shape)
        cum_values[1:] = np.cumsum(hist * np.diff(bin_edges))
        cdf = interpolate.interp1d(cum_values, bin_edges)
        pdf = interpolate.interp1d(bin_edges, cum_values)
        x_min, x_max = cdf([0, 0.99999999])
        return pdf(np.arange(x_min, x_max, (x_max - x_min) / bins))

    @staticmethod
    def _produce_probability_distribution2(rv):
        # probability
        rv_sum = sum(rv)
        rv_p = [_ / rv_sum for _ in rv]
        # accumulated probability
        rv_p_accumulate = copy.copy(rv_p)
        for i in range(1, len(rv_p_accumulate)):
            rv_p_accumulate[i] = rv_p_accumulate[i - 1] + rv_p_accumulate[i]
        return rv_p_accumulate

    @staticmethod
    def new(config_object, objects, kind='coarse'):
        # kind: nature | chunk
        logger = logging.getLogger('.'.join([__name__, SelectionModel.__class__.__name__]))
        #logger.info('Generating distribution')
        logger.debug(config_object)
        logger.debug("key")
        # print("@", config_object['type'].lower())
        distribution_type = DistributionType[config_object['type'].lower()]
        n_samples = 1000000  # should be accurate enough

        params = []
        statistics_distribution = None
        if distribution_type == DistributionType.fixed:
            return SelectionModel.new_fixed(objects, percentages=config_object['percentage'], n_samples=n_samples)
        elif distribution_type == DistributionType.uniform:
            params = []
            statistics_distribution = stats.uniform;
        elif distribution_type == DistributionType.normal:
            params = []
            statistics_distribution = stats.norm
        elif distribution_type == DistributionType.exponential:
            params = []
            statistics_distribution = stats.expon
        elif distribution_type == DistributionType.lognormal:
            params = [config_object['shape']]
            statistics_distribution = stats.lognorm
        elif distribution_type == DistributionType.beta:
            params = [config_object['alpha'], config_object['beta']]
            statistics_distribution = stats.beta
        elif distribution_type == DistributionType.gamma:
            params = [config_object['shape']]
            statistics_distribution = stats.gamma
        elif distribution_type == DistributionType.pareto:
            params = [config_object['shape']]
            statistics_distribution = stats.pareto
        elif distribution_type == DistributionType.powerlaw:
            params = [config_object['shape']]
            statistics_distribution = stats.powerlaw

        else:
            raise Exception("Undefined distribution type")

        kind = config_object['kind'] if 'kind' in config_object else 'nature'
        if kind == 'nature':
            x_min = statistics_distribution.ppf(0.0000001, *params)
            x_max = statistics_distribution.ppf(0.9999999, *params)
            probability_list = statistics_distribution.cdf(np.arange(x_min, x_max, (x_max - x_min) / len(objects)), *params)
            return SelectionModel(distribution_type, probability_list, objects, param=params)
        elif kind == 'chunk':
            #print('chunck model..........')
            num_chunks = int(config_object['num_chunks'])
            x_min = statistics_distribution.ppf(0.0000001, *params)
            x_max = statistics_distribution.ppf(0.9999999, *params)
            x_list = [x_min + (x_max - x_min) / num_chunks * i for i in range(1, num_chunks + 1)]
            cdf_list = statistics_distribution.cdf(x_list, *params)
            cdf_list = np.append([0], cdf_list)
            pdf_list = sorted(np.diff(cdf_list), reverse=True)
            probability_list = pdf_list
            for i in range(len(probability_list) - 1):
                probability_list[i + 1] += probability_list[i]
            probability_list[-1] = 1.0
            #print(probability_list)
            #cdf_diff = np.diff(cdf_list)
            #print('before)', cdf_list)
            #reversed_cdf_list = list(reversed([abs(n-1) for n in cdf_list]))
            #print('after)', list(reversed_cdf_list))
            object_chunks = collection_helper.split_list(objects, num_chunks)
            return ChunkSelectionModel(distribution_type, probability_list, object_chunks, param=params)

    @staticmethod
    def new_fixed(objects, percentages):
        if type(objects) is not type(percentages):
            raise Exception("the parameters need to have the same type: either dict or list")
        if type(objects) is list:
            # not sure about the data type here
            probability_list = [float(p) for p in percentages]
            for i in range(1, len(probability_list)):
                probability_list[i] +=  probability_list[i-1]
            return SelectionModel(DistributionType.fixed, probability_list, objects)
        elif type(objects) is dict:
            object_list = []
            probability_list = []
            for key in objects.keys():
                object_list.append(objects[key])
                probability_list.append(float(percentages[key]))
            for i in range(1, len(probability_list)):
                probability_list[i] += probability_list[i - 1]
            return SelectionModel(DistributionType.fixed, probability_list, object_list)

    @staticmethod
    def new_uniform(objects, n_samples=100000):
        x_min = stats.uniform.ppf(0.0000001)
        x_max = stats.uniform.ppf(0.9999999)
        probability_list = stats.uniform.cdf(np.arange(x_min, x_max, (x_max - x_min) / len(objects)))
        #rv_list = [random.uniform(0, 1) for _ in range(n_samples)]
        #probability_list = SelectionModel._produce_probability_distribution(rv_list, len(objects))
        return SelectionModel(DistributionType.uniform, probability_list, objects, param=[0, 1])

    @staticmethod
    def new_gauss(objects, mu=1, sigma=0.2, n_samples=100000):
        x_min = stats.norm.ppf(0.0000001)
        x_max = stats.norm.ppf(0.9999999)
        probability_list = stats.norm.cdf(np.arange(x_min, x_max, (x_max - x_min) / len(objects)))
        #rv_list = [random.gauss(mu, sigma) for _ in range(n_samples)]
        #probability_list = SelectionModel._produce_probability_distribution(rv_list, len(objects))
        return SelectionModel(DistributionType.gaussian, probability_list, objects, param=[mu, sigma])

    @staticmethod
    def new_exponential(objects, lambd=1, n_samples=100000):
        x_min = stats.expon.ppf(0.0000001)
        x_max = stats.expon.ppf(0.9999999)
        probability_list = stats.expon.cdf(np.arange(x_min, x_max, (x_max - x_min) / len(objects)))
        #rv_list = [random.expovariate(lambd) for _ in range(n_samples)]
        #probability_list = SelectionModel._produce_probability_distribution(rv_list, len(objects))
        return SelectionModel(DistributionType.exponential, probability_list, objects, param=[lambd])

    @staticmethod
    def new_longnormal(objects, shape=1.0, n_samples=100000):
        x_min = stats.lognorm.ppf(0.0000001, shape)
        x_max = stats.lognorm.ppf(0.9999999, shape)
        probability_list = stats.lognorm.cdf(np.arange(x_min, x_max, (x_max - x_min) / len(objects)), shape)
        #rv_list = [random.lognormvariate(mu, sigma) for _ in range(n_samples)]
        #probability_list = SelectionModel._produce_probability_distribution(rv_list, len(objects))
        return SelectionModel(DistributionType.longnormal, probability_list, objects, param=[shape])

    @staticmethod
    def new_beta(objects, alpha=2, beta=1, n_samples=100000):
        x_min = stats.beta.ppf(0.0000001, alpha, beta)
        x_max = stats.beta.ppf(0.9999999, alpha, beta)
        probability_list = stats.beta.cdf(np.arange(x_min, x_max, (x_max - x_min) / 1000), alpha, beta)
        #rv_list = [random.betavariate(alpha, beta) for _ in range(n_samples)]
        #probability_list = SelectionModel._produce_probability_distribution(rv_list, len(objects))
        return SelectionModel(DistributionType.beta, probability_list, objects, param=[alpha, beta])

    @staticmethod
    def new_gamma(objects, shape=2, n_samples=100000):
        x_min = stats.gamma.ppf(0.0000001, shape)
        x_max = stats.gamma.ppf(0.9999999, shape)
        probability_list = stats.gamma.cdf(np.arange(x_min, x_max, (x_max - x_min) / 1000), shape)
        #rv_list = [random.gammavariate(alpha, beta) for _ in range(n_samples)]
        #probability_list = SelectionModel._produce_probability_distribution(rv_list, len(objects))
        return SelectionModel(DistributionType.gamma, probability_list, objects, param=[shape])

    @staticmethod
    def new_pareto(objects, shape=100, n_samples=100000):
        x_min = stats.pareto.ppf(0.0000001, shape)
        x_max = stats.pareto.ppf(0.9999999, shape)
        probability_list = stats.pareto.cdf(np.arange(x_min, x_max, (x_max - x_min) / 1000), shape)
        #rv_list = [random.paretovariate(alpha) for _ in range(n_samples)]
        #probability_list = SelectionModel._produce_probability_distribution(rv_list, len(objects))
        return SelectionModel(DistributionType.pareto, probability_list, objects, param=[shape])

    def __repr__(self):
        paremeter_str = "_".join([str(s) for s in self.kwargs['param']])
        return "".join([str(self.__class__.__name__), "[type=", repr(self.distribution), "[param=", paremeter_str, "[obj_key=", repr(self.key_list[0]), "[obj_val=", repr(self.objects[self.key_list[0]]), str(len(self.key_list))])

    def __eq__(self, other):
        return self.__repr__() == other.__repr__()

    def __hash__(self):
        return helper.md5hash(self.__repr__())


class ChunkSelectionModel:

    def __init__(self, distribution_type, probability_list, object_records, **kwargs):
        self.distribution = distribution_type
        self.probability_list = probability_list
        self.object_records = object_records
        # TODO: not efficient?
        # self.key_list = list(self.objects.keys()) if type(self.objects) is dict else list(range(len(objects)))
        self.kwargs = kwargs
        #print(probability_list)

    def select(self):
        chunk_index = self.select_chunk()
        chunk = self.object_records[chunk_index]
        return chunk[random.randint(0, len(chunk)-1)]

    def select_chunk(self):
        rv = random.random()
        chunk_index = bisect.bisect_left(self.probability_list, rv)
        #print(rv, chunk_index)
        return chunk_index

class WorkloadItem:
    def __init__(self, wid, query_name, endpoint, query_key, key):
        self.wid = wid
        self.query_name = query_name
        self.endpoint = endpoint
        self.query_key = query_key
        self.key = key


class WorkloadExecutionTimeline:

    @staticmethod
    def from_workload(workload):
        logger = logging.getLogger('.'.join([__name__, WorkloadExecutionTimeline.__class__.__name__]))
        workload.init()
        dispatch_mode = workload.dispatch_mode
        workload_timeline = {}
        current_time = 0
        logger.info('dispath: {}'.format(dispatch_mode))
        if dispatch_mode == 'once':
            current_time = 0
            workload_timeline[current_time] = []  # just one time slot
            num_queries = workload.next()
            while num_queries is not None:
                for i in range(num_queries):
                    app = workload.application_selection.select()
                    wid = "{}-{}".format(current_time + 1, len(workload_timeline[current_time]) + 1)
                    workload_item = WorkloadItem(wid, *app.next_query())
                    workload_timeline[current_time].append(workload_item)
                num_queries = workload.next()
            return WorkloadExecutionTimeline(1, workload_timeline)
        else:
            num_queries = workload.next()
            while num_queries is not None:
                if current_time not in workload_timeline:
                    workload_timeline[current_time] = []
                for i in range(num_queries):
                    app = workload.application_selection.select()
                    wid = "{}-{}".format(current_time+1, i+1)
                    workload_item = WorkloadItem(wid, *app.next_query())
                    workload_timeline[current_time].append(workload_item)
                num_queries = workload.next()
                current_time += 1
            return WorkloadExecutionTimeline(workload.period, workload_timeline)

    @staticmethod
    def from_timeline(timeline, workload):
        new_workload_timeline = {}
        for t in sorted(timeline.timeline.keys()):
            new_workload_timeline[t] = []
            num_queries = len(timeline.timeline[t])
            for i in range(num_queries):
                app = workload.application_selection.select()
                wid = "{}-{}".format(t + 1, i + 1)
                workload_item = WorkloadItem(wid, *app.next_query())
                new_workload_timeline[t].append(workload_item)
        return WorkloadExecutionTimeline(workload.period, new_workload_timeline)

    @staticmethod
    def from_pickle(file_path):
        with open(file_path, 'rb') as f:
            return pickle.load(f)

    def __init__(self, period, timeline):
        self.period = period
        self.timeline = timeline

    def next(self):
        for i in range(self.period):
            yield (i, self.timeline[i])

    def to_pickle(self, output_path):
        with open(output_path, 'wb') as f:
            pickle.dump(self, f)


class RoxieApplication:
    def __init__(self, query_name, endpoint_list, query_key, selection_model):
        self.query_name = query_name
        self.endpoint_list = endpoint_list
        self.query_key = query_key
        self.selection_model = selection_model
        self.endpoint_dispatcher = RoxieTargetDispatcher(self.endpoint_list)

    def next_query(self):
        endpoint = self.endpoint_dispatcher.next()
        if "8002" in endpoint:
            endpoint = "{}/WsEcl/json/query/roxie/{}".format(endpoint, self.query_name)
        return self.query_name, endpoint, self.query_key, self.selection_model.select()

    def _keys_md5(self):
        m = hashlib.md5()
        for key in self.selection_model.objects:
            m.update(key.encode())
        return m.hexdigest()

    def __repr__(self):
        return "".join([str(self.__class__.__name__), "[type=", self.query_name, str(len(self.endpoint_list)), repr(self.selection_model.distribution), self._keys_md5()])

    def __eq__(self, other):
        return self.__repr__() == other.__repr__()

    def __hash__(self):
        return helper.md5hash(self.__repr__())
