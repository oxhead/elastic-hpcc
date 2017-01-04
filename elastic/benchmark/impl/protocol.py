import logging
from enum import Enum


class SendProtocolHeader(Enum):
    admin_start = 0
    admin_stop = 1
    admin_status = 2
    admin_update_workload = 3
    admin_update_routing_table = 4

    internal_heartbeat = 11
    internal_register = 12
    internal_echo = 13
    internal_ready = 14
    internal_retrieve_jobs = 15

    workload_submit = 21
    workload_status = 22
    workload_report = 23
    workload_statistics = 24
    workload_timeline_completion = 25
    workload_timeline_failure = 26
    workload_download = 27

    report_done = 31

    routing_table_upload = 41


class ReceiveProtocolHeader(Enum):
    successful = 0
    failed = 1


class BenchmarkProtocol:
    @staticmethod
    def new_procotol(header, *payloads):
        return BenchmarkProtocol(header, payloads)

    def __init__(self, header, payloads=[]):
        self.header = header
        self.payloads = payloads


class BenchmarkReporterProtocol:
    def __init__(self, driver_id, result_sender):
        self.driver_id = driver_id
        self.result_sender = result_sender
        self.logger = logging.getLogger('.'.join([__name__, self.__class__.__name__]))

    def report_statistic_list(self, report_statistic_list):
        protocol = BenchmarkProtocol.new_procotol(SendProtocolHeader.report_done, self.driver_id, report_statistic_list)
        self.result_sender.send_pyobj(protocol)

    def report(self, worker_item, queue_timestamp, start_timestemp, finish_timestamp, success, output_size, status_code, exception_description):
        statics = {
            "item": worker_item,
            "queueTimestamp": queue_timestamp,
            "startTimestamp": start_timestemp,
            "finishTimestamp": finish_timestamp,
            "success": success,
            "size": output_size,
            "status": status_code,
            "exception": exception_description
        }
        protocol = BenchmarkProtocol.new_procotol(SendProtocolHeader.report_done, statics)
        self.result_sender.send_pyobj(protocol)
        self.logger.info("report completion: wid=%s, runningTime=%s, totalTime=%s", worker_item, (finish_timestamp-start_timestemp), (finish_timestamp-queue_timestamp))


class BenchmarkBaseProtocol:
    def __init__(self, target_socket, timeout=3000):
        self.logger = logging.getLogger('.'.join([__name__, self.__class__.__name__]))
        self.target_socket = target_socket
        self.timeout = timeout

    def _send(self, header, *payloads):
        self.logger.info("header={}, payloads={}".format(header, payloads))
        request_protocol = BenchmarkProtocol(header, payloads)
        self.target_socket.send_pyobj(request_protocol)
        reply_protocol = self.target_socket.recv_pyobj()
        if reply_protocol.header is not ReceiveProtocolHeader.successful:
            raise Exception("unable to complete the request")
        # self.logger.info(reply_protocol.payloads[0])
        # should be have one return value
        return reply_protocol.payloads[0]


class BenchmarkInternalProtocol(BenchmarkBaseProtocol):
    def __init__(self, driver_id, internal_socket, timeout=3000):
        super().__init__(internal_socket, timeout=timeout)
        self.driver_id = driver_id

    def register(self):
        self._send(SendProtocolHeader.internal_register, self.driver_id)

    def ready(self):
        self._send(SendProtocolHeader.internal_echo, self.driver_id)

    def ready(self):
        self._send(SendProtocolHeader.internal_ready, self.driver_id)

    def heartbeat(self, status):
        return self._send(SendProtocolHeader.internal_heartbeat, self.driver_id, status)

    def retrieve_jobs(self, num_jobs=10):
        return self._send(SendProtocolHeader.internal_retrieve_jobs, self.driver_id, num_jobs)


class BenchmarkClientProtocol(BenchmarkBaseProtocol):
    def __init__(self, client_socket, timeout=3000):
        super().__init__(client_socket, timeout=timeout)

    def status(self):
        return self._send(SendProtocolHeader.admin_status)

    def stop(self):
        self._send(SendProtocolHeader.admin_stop)

    def routing_table_upload(self, routing_table):
        self.logger.info("upload routing table")
        return self._send(SendProtocolHeader.routing_table_upload, routing_table)

    def workload_submit(self, workload):
        self.logger.info("submit workload")
        return self._send(SendProtocolHeader.workload_submit, workload)

    def workload_report(self, workload_id):
        self.logger.info("retrieve workload report")
        return self._send(SendProtocolHeader.workload_report, workload_id)

    def workload_statistics(self, workload_id):
        self.logger.info("retrieve workload statistics")
        return self._send(SendProtocolHeader.workload_statistics, workload_id)

    def workload_timeline_completion(self, workload_id):
        self.logger.info("retrieve completion timeline")
        return self._send(SendProtocolHeader.workload_timeline_completion, workload_id)

    def workload_timeline_failure(self, workload_id):
        self.logger.info("retrieve failure timeline")
        return self._send(SendProtocolHeader.workload_timeline_failure, workload_id)

    def workload_status(self, workload_id):
        self.logger.info("check workload status")
        return self._send(SendProtocolHeader.workload_status, workload_id)

    def workload_download(self, workload_id):
        return self._send(SendProtocolHeader.workload_download, workload_id)


