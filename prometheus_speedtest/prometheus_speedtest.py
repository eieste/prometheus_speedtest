#!/usr/bin/python3.7
"""Instrument speedtest.net speedtests from Prometheus."""

from typing import Optional
from typing import Sequence
from http import server
from urllib.parse import urlparse
import os

from absl import app
from absl import flags
from absl import logging
from prometheus_client import core
import prometheus_client
import speedtest
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from prometheus_speedtest import version
from prometheus_client.parser import text_string_to_metric_families


flags.DEFINE_string('address', '0.0.0.0', 'address to listen on')
flags.DEFINE_integer('port', 9516, 'port to listen on')
flags.DEFINE_list(
    'servers', [],
    'speedtest server(s) to use - leave empty for auto-selection')
flags.DEFINE_list(
    'excludes', [],
    'speedtest server(s) to exclude - leave empty for no exclusion')
flags.DEFINE_list('remotes', [], 'additional speedtest trigger')

flags.DEFINE_boolean('version', False, 'show version')
FLAGS = flags.FLAGS


class SpeedtestData:

    def __init__(self):
        self.download_speed_bps = 0
        self.upload_speed_bps = 0
        self.ping_ms = 0
        self.bytes_received = 0
        self.bytes_sent = 0
        self.count = 0

    def add(self, download_speed_bps, upload_speed_bps, ping_ms, bytes_received, bytes_sent):
        self.download_speed_bps += download_speed_bps
        self.upload_speed_bps += upload_speed_bps
        self.ping_ms += ping_ms
        self.bytes_received +=  bytes_received
        self.bytes_sent += bytes_sent
        self.count += 1


class PrometheusSpeedtest():
    """Enapsulates behavior performing and reporting results of speedtests."""
    def __init__(self,
                 source_address: Optional[str] = None,
                 timeout: int = 10,
                 servers: Optional[Sequence[str]] = None,
                 excludes: Optional[Sequence[str]] = None):
        """Instantiates a PrometheusSpeedtest object.

        Args:
            source_address: str - optional network address to bind to.
                e.g. 192.168.1.1.
            timeout: int - optional timeout for speedtest in seconds.
            servers: list of speedtest server ids to restrict testing to.
            servers: list of speedtest server ids to exclude testing from.
        """
        self._source_address = source_address
        self._timeout = timeout
        self._servers = servers
        self._excludes = excludes

    def test(self):
        """Performs speedtest, returns results.

        Returns:
            speedtest.SpeedtestResults object.
        """
        logging.info('Performing Speedtest...')
        client = speedtest.Speedtest(source_address=self._source_address,
                                     timeout=self._timeout)
        logging.debug(
            'Eligible servers: %s',
            client.get_servers(servers=self._servers, exclude=self._excludes))
        logging.debug('Picked server: %s', client.get_best_server())
        client.download()
        client.upload()
        logging.info('Results: %s', client.results)
        return client.results


class SpeedtestCollector():
    """Performs Speedtests when requested from Prometheus."""
    def __init__(self,
                 tester: Optional[PrometheusSpeedtest] = None,
                 servers: Optional[Sequence[str]] = None,
                 excludes: Optional[Sequence[str]] = None):
        """Instantiates a SpeedtestCollector object.

        Args:
            tester: An instantiated PrometheusSpeedtest object for testing.
            servers: servers-id to use when tester is auto-created
        """
        self._tester = tester if tester else PrometheusSpeedtest(
            servers=servers, excludes=excludes)

    def collect(self):
        """Performs a Speedtests and yields metrics.

        Yields:
            core.Metric objects.
        """
        results = self._tester.test()
        return {
            "download_speed_bps": results.download,
            "upload_speed_bps": results.upload,
            "ping_ms": results.ping,
            "bytes_received": results.bytes_received,
            "bytes_sent": results.bytes_sent,
        }
        #
        # download_speed = core.GaugeMetricFamily('download_speed_bps',
        #                                         'Download speed (bit/s)')
        # download_speed.add_metric(labels=[], value=results.download)
        # yield download_speed
        #
        # upload_speed = core.GaugeMetricFamily('upload_speed_bps',
        #                                       'Upload speed (bit/s)')
        # upload_speed.add_metric(labels=[], value=results.upload)
        # yield upload_speed
        #
        # ping = core.GaugeMetricFamily('ping_ms', 'Latency (ms)')
        # ping.add_metric(labels=[], value=results.ping)
        # yield ping
        #
        # bytes_received = core.GaugeMetricFamily('bytes_received',
        #                                         'Bytes received during test')
        # bytes_received.add_metric(labels=[], value=results.bytes_received)
        # yield bytes_received
        #
        # bytes_sent = core.GaugeMetricFamily('bytes_sent',
        #                                     'Bytes sent during test')
        # bytes_sent.add_metric(labels=[], value=results.bytes_sent)
        # yield bytes_sent


class RemoteSpeedtestCollector():
    """
        Trigger remote Speedtests
    """

    """Performs Speedtests when requested from Prometheus."""
    def __init__(self,
                 tester: Optional[PrometheusSpeedtest] = None,
                 servers: Optional[Sequence[str]] = None,
                 excludes: Optional[Sequence[str]] = None,
                 remotes: Optional[Sequence[str]] = None):
        """Instantiates a SpeedtestCollector object.

        Args:
            tester: An instantiated PrometheusSpeedtest object for testing.
            servers: servers-id to use when tester is auto-created
        """
        self._remotes = remotes
        self._tester = tester
        self._servers = servers
        self._excludes = excludes

    def collect(self):

        speedtest_data = SpeedtestData()
        result = []

        with ThreadPoolExecutor(max_workers=5) as executor:
            local_speed_test = SpeedtestCollector(tester=self._tester, servers=self._servers, excludes=self._excludes)
            result.append(executor.submit(local_speed_test.collect))

            for url in self._remotes:
                result.append(executor.submit(remote_collector, url))

                for future in as_completed(result):
                    speedtest_data.add(**future.result())

        speedtest_devices = core.CounterMetricFamily('speedtest_devices', 'Count of devices used for Speedtest-Results (pc)')
        speedtest_devices.add_metric(labels=[], value=speedtest_data.count)
        yield speedtest_devices

        download_speed = core.GaugeMetricFamily('download_speed_bps',
                                                'Download speed (bit/s)')
        download_speed.add_metric(labels=[], value=speedtest_data.download_speed_bps)
        yield download_speed

        upload_speed = core.GaugeMetricFamily('upload_speed_bps',
                                              'Upload speed (bit/s)')
        upload_speed.add_metric(labels=[], value=speedtest_data.upload_speed_bps)
        yield upload_speed

        ping = core.GaugeMetricFamily('ping_ms', 'Latency Average (ms)')
        ping.add_metric(labels=[], value=speedtest_data.ping_ms/speedtest_data.count)
        yield ping

        bytes_received = core.GaugeMetricFamily('bytes_received',
                                                'Bytes received during test')
        bytes_received.add_metric(labels=[], value=speedtest_data.bytes_received)
        yield bytes_received

        bytes_sent = core.GaugeMetricFamily('bytes_sent',
                                            'Bytes sent during test')
        bytes_sent.add_metric(labels=[], value=speedtest_data.bytes_sent)
        yield bytes_sent

def remote_collector(url):
    logging.info('Fetch from URL: %s', url)
    metrics = requests.get(url).text
    result = {}
    for family in text_string_to_metric_families(metrics):
      for sample in family.samples:
          result.update({sample[0]: sample[2]})
    logging.info('Results: %s', result)
    return result


class SpeedtestMetricsHandler(server.SimpleHTTPRequestHandler,
                              prometheus_client.MetricsHandler):
    """HTTP handler extending MetricsHandler and adding status page support."""
    def __init__(self, *args, **kwargs):
        static_directory = os.path.join(os.path.dirname(__file__), 'static')
        # pylint: disable=unexpected-keyword-arg
        super().__init__(directory=static_directory, *args, **kwargs)
        # pylint: enable=unexpected-keyword-arg

    def do_GET(self):
        """Handles HTTP GET requests.

        Requests to '/probe' are handled by prometheus_client.MetricsHandler,
        other requests serve static HTML.
        """
        logging.info('%s - %s', self.requestline, self.client_address)
        path = urlparse(self.path).path
        if path == '/probe':
            prometheus_client.MetricsHandler.do_GET(self)
        else:
            server.SimpleHTTPRequestHandler.do_GET(self)


def main(argv):
    """Entry point for prometheus_speedtest.py."""
    del argv  # unused
    if FLAGS.version:
        print('prometheus_speedtest v%s' % version.VERSION)
        return

    registry = core.CollectorRegistry(auto_describe=False)
    if FLAGS.servers and set(FLAGS.excludes).issuperset(FLAGS.servers):
        logging.fatal(
            '--excludes is a superset of --includes, no viable test server is '
            'possible. Ensure --excludes does not contain all --servers.')

    # TEST_ASDF_ASDF_ASDF_ASDF_ASDF_ASDF_ASDF_ASDF_ASDF_ASDFfoobar

    registry.register(RemoteSpeedtestCollector(servers=FLAGS.servers,
                                         excludes=FLAGS.excludes,
                                         remotes=FLAGS.remotes)
                                         )

    metrics_handler = SpeedtestMetricsHandler.factory(registry)

    http = server.ThreadingHTTPServer((FLAGS.address, FLAGS.port),
                                      metrics_handler)

    logging.info('Starting HTTP server listening on %s:%s', FLAGS.address,
                 FLAGS.port)
    http.serve_forever()


def init():
    """Initializes the prometheus_speedtest cli."""
    app.run(main)


if __name__ == '__main__':
    init()
