#!/usr/bin/env python
# -*- coding: utf-8 *-*
import argparse
import fnmatch
import os
import os.path as path
import json
from builtins import staticmethod

import requests
import urllib3
from datetime import datetime, timedelta

import nagiosplugin as np

""" 
    Copyright 2019 VisualVest   
    Copyright 2015 predic8 GmbH, www.predic8.com

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License. """

PLUGIN_VERSION = "0.8"
PREFIX = 'org.apache.activemq.artemis:'
args_timeout = 5

urllib3.disable_warnings()


def make_url(args, dest):
    return (
        (args.jolokia_url + ('' if args.jolokia_url[-1] == '/' else '/') + dest)
        if args.jolokia_url
        else (("https://" if args.ssl else "http://") + args.user + ':' + args.pwd
              + '@' + args.host + ':' + str(args.port)
              + '/' + args.url_tail + '/' + dest)
    )


def queues_url(args):
    json_params = json.dumps({"operation": "", "field": "", "value": ""})
    return query_url(args, operation='exec',
                     dest='/listQueues(java.lang.String,int,int)/{}/{}/{}'.format(json_params, 1, 1000))


def query_url(args, operation='read', dest=''):
    return make_url(args, operation + '/' + PREFIX + 'broker="' + args.brokerName + '"' + dest)


def queue_url(args, queue):
    query = ',component=addresses,address="{}",subcomponent=queues,routing-type="anycast",queue="{}"'.format(queue,
                                                                                                             queue)
    return query_url(args, 'read', query)


def topic_url(args, topic):
    query = ',component=addresses,address="{}",subcomponent=queues,routing-type="multicast",queue="{}"'.format(topic,
                                                                                                               topic)
    return query_url(args, 'read', query)


def health_url(args):
    return query_url(args, 'read', '/Started')


def load_json(url):
    try:
        r = requests.get(url, timeout=args_timeout, verify=False)
        return r.json() if r.status_code == requests.codes.ok else None
    except:
        return None


def parse_iso_date(iso_date_string):
    k = iso_date_string.rfind(":")
    iso_date_string = iso_date_string[:k] + iso_date_string[k + 1:]
    return datetime.strptime(iso_date_string, "%Y-%m-%dT%H:%M:%S%z")


def queue_oldest_msg_timestamp(args, queue):
    query = ',component=addresses,address="{}",subcomponent=queues,routing-type="anycast",queue="{}"/browse(int,int)/{}/{}'.format(
        queue, queue, 1, 1)
    messages = load_json(query_url(args, operation='exec', dest=query))

    if not (messages and messages['value']):
        # yield np.Metric('Getting Queue(s) FAILED: failed response', -1, context='age')
        return

    if len(messages['value']) == 0:
        return None
    else:
        return parse_iso_date(messages['value'][0]['timestamp'])


def queue_age(args):
    class ActiveMqQueueAgeContext(np.ScalarContext):

        def evaluate(self, metric, resource):
            if metric.value < 0:
                return self.result_cls(np.Unknown, metric=metric)

            delta = timedelta(minutes=metric.value)
            # CRITICAL
            if metric.value >= self.critical.end:
                if args.queue:
                    return self.result_cls(np.Critical, 'Queue %s is %s old (W=%d,C=%d)' %
                                           (args.queue, delta, self.warning.end, self.critical.end), metric)
                else:
                    return self.result_cls(np.Critical, 'Some queue is %s old (W=%d,C=%d)' %
                                           (delta, self.warning.end, self.critical.end), metric)

            # WARNING
            if metric.value >= self.warning.end:
                if args.queue:
                    return self.result_cls(np.Warn, 'Queue %s is %s old (W=%d,C=%d)' %
                                           (args.queue, delta, self.warning.end, self.critical.end), metric)
                else:
                    return self.result_cls(np.Warn, 'Some queue is %s old (W=%d,C=%d)' %
                                           (delta, self.warning.end, self.critical.end), metric)

            # OK
            if args.queue:
                return self.result_cls(np.Ok, 'Queue %s is %s old (W=%d,C=%d)' %
                                       (args.queue, delta, self.warning.end, self.critical.end), metric)
            else:
                return self.result_cls(np.Ok, 'All queues are younger than %d minutes (W=%d,C=%d)' %
                                       (self.warning.end, self.warning.end, self.critical.end), metric)

        def describe(self, metric):
            if metric.value < 0:
                return 'ERROR: ' + metric.name
            return super(ActiveMqQueueAgeContext, self).describe(metric)

        @staticmethod
        def fmt_violation(max_value):
            queue_name = args.queue if args.queue else '<some queues>'
            return 'Queue %s older than %d minutes' % (queue_name, max_value)

    class ActiveMqQueueAge(np.Resource):
        def __init__(self, pattern=None):
            self.pattern = pattern

        def probe(self):
            try:
                now = datetime.now().astimezone()
                queues_json = load_json(queues_url(args))
                if not (queues_json and queues_json['value']):
                    yield np.Metric('Getting Queue(s) FAILED: failed response', -1, context='age')
                    return
                for queue in json.loads(queues_json['value'])['data']:
                    queue_name = queue['name']
                    if self.pattern and fnmatch.fnmatch(queue_name, self.pattern) or not self.pattern:
                        queue_oldest_time = queue_oldest_msg_timestamp(args, queue_name)
                        queue_oldest_time = queue_oldest_time if queue_oldest_time else now
                        queue_age_minutes = int((now - queue_oldest_time).total_seconds() / 60)

                        yield np.Metric('Minutes', queue_age_minutes, min=0, context='age')
            except IOError as e:
                yield np.Metric('Fetching network FAILED: ' + str(e), -1, context='age')
            except ValueError as e:
                yield np.Metric('Decoding Json FAILED: ' + str(e), -1, context='age')
            except KeyError as e:
                yield np.Metric('Getting Queue(s) FAILED: ' + str(e), -1, context='age')
            except Exception as e:
                yield np.Metric('Unexpected Error: ' + str(e), -1, context='age')

    class ActiveMqQueueAgeSummary(np.Summary):
        def ok(self, results):
            return results.first_significant.hint + (
                    ' BUT values retrieved via JSP pages due to: %s' % '')

        def problem(self, results):
            return results.first_significant.hint if results.first_significant.hint else "Could not retrieve data"

    np.Check(
        ActiveMqQueueAge(args.queue) if args.queue else ActiveMqQueueAge(),
        ActiveMqQueueAgeContext('age', args.warn, args.crit),
        ActiveMqQueueAgeSummary()
    ).main(timeout=args_timeout + 1)


def queue_size(args):
    class ActiveMqQueueSizeContext(np.ScalarContext):
        def evaluate(self, metric, resource):
            if metric.value < 0:
                return self.result_cls(np.Unknown, metric=metric)

            if metric.value >= self.critical.end:
                return self.result_cls(np.Critical, ActiveMqQueueSizeContext.fmt_violation(self.critical.end), metric)

            if metric.value >= self.warning.end:
                return self.result_cls(np.Warn, ActiveMqQueueSizeContext.fmt_violation(self.warning.end), metric)

            return self.result_cls(np.Ok, None, metric)

        def describe(self, metric):
            if metric.value < 0:
                return 'ERROR: ' + metric.name
            return super(ActiveMqQueueSizeContext, self).describe(metric)

        @staticmethod
        def fmt_violation(max_value):
            return 'Queue size is greater than or equal to %d' % max_value

    class ActiveMqQueueSize(np.Resource):
        def __init__(self, pattern=None):
            self.pattern = pattern

        def probe(self):
            try:
                queues_json = load_json(queues_url(args))
                if not (queues_json and queues_json['value']):
                    yield np.Metric('Getting Queue(s) FAILED: failed response', -1, context='size')
                    return
                for queue in json.loads(queues_json['value'])['data']:
                    queue_name = queue['name']
                    if self.pattern and fnmatch.fnmatch(queue_name, self.pattern) or not self.pattern:
                        yield np.Metric('Queue Size of %s' % queue_name,
                                        int(queue['messageCount']), min=0, context='size')
            except IOError as e:
                yield np.Metric('Fetching network FAILED: ' + str(e), -1, context='size')
            except ValueError as e:
                yield np.Metric('Decoding Json FAILED: ' + str(e), -1, context='size')
            except KeyError as e:
                yield np.Metric('Getting Queue(s) FAILED: ' + str(e), -1, context='size')

    class ActiveMqQueueSizeSummary(np.Summary):
        def ok(self, results):
            if len(results) > 1:
                lenQ = str(len(results))
                minQ = str(min([r.metric.value for r in results]))
                avgQ = str(sum([r.metric.value for r in results]) / len(results))
                maxQ = str(max([r.metric.value for r in results]))
                return ('Checked ' + lenQ + ' queues with lengths min/avg/max = '
                        + '/'.join([minQ, avgQ, maxQ]))
            else:
                return super(ActiveMqQueueSizeSummary, self).ok(results)

    np.Check(
        ActiveMqQueueSize(args.queue) if args.queue else ActiveMqQueueSize(),
        ActiveMqQueueSizeContext('size', args.warn, args.crit),
        ActiveMqQueueSizeSummary()
    ).main(timeout=args_timeout)


def health(args):
    class ActiveMqHealthContext(np.Context):
        def evaluate(self, metric, resource):
            if metric.value:
                return self.result_cls(np.Ok, metric=metric)
            else:
                return self.result_cls(np.Critical, metric=metric)

        def describe(self, metric):
            if metric.value < 0:
                return 'ERROR: ' + metric.name
            return metric.name + ' ' + str(metric.value)

    class ActiveMqHealth(np.Resource):
        def probe(self):
            try:
                status = load_json(health_url(args))['value']
                return np.Metric('Started', status, context='health')
            except IOError as e:
                return np.Metric('Fetching network FAILED: ' + str(e), -1, context='health')
            except ValueError as e:
                return np.Metric('Decoding Json FAILED: ' + str(e), -1, context='health')
            except KeyError as e:
                return np.Metric('Getting Values FAILED: ' + str(e), -1, context='health')

    np.Check(
        ActiveMqHealth(),
        ActiveMqHealthContext('health')
    ).main(timeout=args_timeout)


def exists(args):
    class ActiveMqExistsContext(np.Context):
        def evaluate(self, metric, resource):
            if metric.value < 0:
                return self.result_cls(np.Unknown, metric=metric)
            if metric.value > 0:
                return self.result_cls(np.Ok, metric=metric)
            return self.result_cls(np.Critical, metric=metric)

        def describe(self, metric):
            if metric.value < 0:
                return 'ERROR: ' + metric.name
            if metric.value == 0:
                return 'Neither Queue nor Topic with name ' + args.name + ' were found!'
            if metric.value == 1:
                return 'Found Queue with name ' + args.name
            if metric.value == 2:
                return 'Found Topic with name ' + args.name
            return super(ActiveMqExistsContext, self).describe(metric)

    class ActiveMqExists(np.Resource):
        def probe(self):
            try:
                response_queue = load_json(queue_url(args, args.name))
                if response_queue['status'] == 200:
                    return np.Metric('exists', 1, context='exists')

                response_topic = load_json(topic_url(args, args.name))
                if response_topic['status'] == 200:
                    return np.Metric('exists', 2, context='exists')

                return np.Metric('exists', 0, context='exists')

            except IOError as e:
                return np.Metric('Network fetching FAILED: ' + str(e), -1, context='exists')
            except ValueError as e:
                return np.Metric('Decoding Json FAILED: ' + str(e), -1, context='exists')
            except KeyError as e:
                return np.Metric('Getting Queue(s) FAILED: ' + str(e), -1, context='exists')

    np.Check(
        ActiveMqExists(),
        ActiveMqExistsContext('exists')
    ).main(timeout=args_timeout)


def dlq(args):
    class ActiveMqDlqScalarContext(np.ScalarContext):
        def evaluate(self, metric, resource):
            if metric.value > 0:
                return self.result_cls(np.Critical, metric=metric)
            else:
                return self.result_cls(np.Ok, metric=metric)

    class ActiveMqDlq(np.Resource):
        def __init__(self, prefix, cachedir):
            super(ActiveMqDlq, self).__init__()
            self.cache = None
            self.cachedir = path.join(path.expanduser(cachedir), 'activemq-nagios-plugin')
            self.cachefile = path.join(self.cachedir, 'dlq-cache.json')
            self.parse_cache()
            self.prefix = prefix

        def parse_cache(self):  # deserialize
            if not os.path.exists(self.cachefile):
                self.cache = {}
            else:
                with open(self.cachefile, 'r') as cachefile:
                    self.cache = json.load(cachefile)

        def write_cache(self):  # serialize
            if not os.path.exists(self.cachedir):
                os.makedirs(self.cachedir)
            with open(self.cachefile, 'w') as cachefile:
                json.dump(self.cache, cachefile)

        def probe(self):
            try:
                queues_json = load_json(queues_url(args))
                if not (queues_json and queues_json['value']):
                    yield np.Metric('Getting Queue(s) FAILED: failed response', -1, context='dlq')
                    return
                for queue in json.loads(queues_json['value'])['data']:
                    queue_name = queue['name']
                    queue_count = int(queue['messageCount'])
                    if queue_name.startswith(self.prefix):
                        old_count = self.cache.get(queue_name)

                        if old_count is None:
                            more = 0
                            msg = 'First check for DLQ'
                        else:
                            assert isinstance(old_count, int)
                            more = queue_count - old_count
                            if more == 0:
                                msg = 'No additional messages in'
                            elif more > 0:
                                msg = 'More messages in'
                            else:  # more < 0
                                msg = 'Less messages in'
                        self.cache[queue_name] = queue_count
                        self.write_cache()
                        yield np.Metric(msg + ' %s' % queue_name,
                                        more, context='dlq')
            except IOError as e:
                yield np.Metric('Fetching network FAILED: ' + str(e), -1, context='dlq')
            except ValueError as e:
                yield np.Metric('Decoding Json FAILED: ' + str(e), -1, context='dlq')
            except KeyError as e:
                yield np.Metric('Getting Queue(s) FAILED: ' + str(e), -1, context='dlq')

    class ActiveMqDlqSummary(np.Summary):
        def ok(self, results):
            if len(results) > 1:
                length_queue = str(len(results))
                bigger = str(len([r.metric.value for r in results if r.metric.value > 0]))
                return 'Checked ' + length_queue + ' DLQs of which ' + bigger + ' contain additional messages.'
            else:
                return super(ActiveMqDlqSummary, self).ok(results)

    np.Check(
        ActiveMqDlq(args.prefix, args.cachedir),
        ActiveMqDlqScalarContext('dlq'),
        ActiveMqDlqSummary()
    ).main(timeout=args_timeout)


def add_warn_crit(parser, what):
    parser.add_argument('-w', '--warn',
                        metavar='WARN', type=int, default=10,
                        help='Warning if ' + what + ' is greater than or equal to. (default: %(default)s)')
    parser.add_argument('-c', '--crit',
                        metavar='CRIT', type=int, default=100,
                        help='Warning if ' + what + ' is greater than or equal to. (default: %(default)s)')


@np.guarded
def main():
    # Top-level Argument Parser & Subparsers Initialization
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument('-v', '--version', action='version',
                        help='Print version number',
                        version='%(prog)s version ' + str(PLUGIN_VERSION)
                        )
    connection = parser.add_argument_group('Connection')
    connection.add_argument('--ssl', action='store_true',
                            help='If the connection is tls secured (default: %(default)s)')
    connection.add_argument('--host', default='localhost',
                            help='ActiveMQ Server Hostname (default: %(default)s)')
    connection.add_argument('--port', type=int, default=8161,
                            help='ActiveMQ Server Port (default: %(default)s)')
    connection.add_argument('--timeout', type=int, default=5,
                            help='Timeout (default: %(default)s)')
    connection.add_argument('-b', '--brokerName', default='localhost',
                            help='Name of your broker. (default: %(default)s)')
    connection.add_argument('--url-tail',
                            default='console/jolokia',
                            help='Jolokia URL tail part. (default: %(default)s)')
    connection.add_argument('-j', '--jolokia-url',
                            help='''Override complete Jolokia URL.
                (Default: "http://USER:PWD@HOST:PORT/URLTAIL/").
                The parameters --user, --pwd, --host and --port are IGNORED
                if this parameter is specified!
                Please set this parameter carefully as it essential
                for the program to work properly and is not validated.''')

    credentials = parser.add_argument_group('Credentials')
    credentials.add_argument('-u', '--user', default='admin',
                             help='Username for ActiveMQ admin account. (default: %(default)s)')
    credentials.add_argument('-p', '--pwd', default='admin',
                             help='Password for ActiveMQ admin account. (default: %(default)s)')

    subparsers = parser.add_subparsers()

    # Sub-Parser for queueage
    parser_queueage = subparsers.add_parser('queueage',
                                            help="""Check QueueAge: This mode checks the queue age of one
                or more queues on the ActiveMQ server.
                You can specify a queue name to check (even a pattern);
                see description of the 'queue' parameter for details.""")
    add_warn_crit(parser_queueage, 'Queue Age')
    parser_queueage.add_argument('queue', nargs='?',
                                 help='''Name of the Queue that will be checked.
                If left empty, all Queues will be checked.
                This also can be a Unix shell-style Wildcard
                (much less powerful than a RegEx)
                where * and ? can be used.''')
    parser_queueage.set_defaults(func=queue_age)

    # Sub-Parser for queuesize
    parser_queuesize = subparsers.add_parser('queuesize',
                                             help="""Check QueueSize: This mode checks the queue size of one
                or more queues on the ActiveMQ server.
                You can specify a queue name to check (even a pattern);
                see description of the 'queue' parameter for details.""")
    add_warn_crit(parser_queuesize, 'Queue Size')
    parser_queuesize.add_argument('queue', nargs='?',
                                  help='''Name of the Queue that will be checked.
                If left empty, all Queues will be checked.
                This also can be a Unix shell-style Wildcard
                (much less powerful than a RegEx)
                where * and ? can be used.''')
    parser_queuesize.set_defaults(func=queue_size)

    # Sub-Parser for health
    parser_health = subparsers.add_parser('health',
                                          help="""Check Health: This mode checks if the current status is 'Good'.""")
    # no additional arguments necessary
    parser_health.set_defaults(func=health)

    # Sub-Parser for exists
    parser_exists = subparsers.add_parser('exists',
                                          help="""Check Exists: This mode checks if a Queue or Topic with the
                given name exists.
                If either a Queue or a Topic with this name exist,
                this mode yields OK.""")
    parser_exists.add_argument('--name', required=True,
                               help='Name of the Queue or Topic that will be checked.')
    parser_exists.set_defaults(func=exists)

    # Sub-Parser for dlq
    parser_dlq = subparsers.add_parser('dlq',
                                       help="""Check DLQ (Dead Letter Queue):
                This mode checks if there are new messages in DLQs
                with the specified prefix.""")
    parser_dlq.add_argument('--prefix',  # required=False,
                            default='DLQ',
                            help='DLQ prefix to check. (default: %(default)s)')
    parser_dlq.add_argument('--cachedir',  # required=False,
                            default='~/.cache',
                            help='DLQ cache base directory. (default: %(default)s)')
    add_warn_crit(parser_dlq, 'DLQ Queue Size')
    parser_dlq.set_defaults(func=dlq)

    # Evaluate Arguments
    args = parser.parse_args()
    # call the determined function with the parsed arguments
    global args_timeout
    args_timeout = args.timeout
    args.func(args)


if __name__ == '__main__':
    main()
