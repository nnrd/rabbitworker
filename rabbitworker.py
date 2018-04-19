#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys
import getopt
import json
import pprint
import subprocess
import signal
import datetime
import time
import pika
import suds
import requests
from requests.auth import HTTPBasicAuth


# Репозиторий объектов
class Registry(object):

    def __init__(self):
        self._store = {}

    def get(self, name):
        return self._store[name]

    def set(self, name, value):
        self._store[name] = value

    def has(self, name):
        return name in self._store


# Хранилище опциий
class Options(object):

    def __init__(self):
        self._store = {}
        self.log = App().registry().get('log')
        self.sys = App().registry().get('sys')

    def get(self, name):
        return self._store[name]

    def set(self, name, value):
        self._store[name] = value

    def has(self, name):
        return name in self._store

    def verbose(self):
        return 'verbose' in self._store and self._store['verbose']

    def debug(self):
        return 'debug' in self._store and self._store['debug']

    def die_on_unknown_command(self):
        return 'die_on_unknown_command' in self._store and self._store['die_on_unknown_command']

    def one_shot(self):
        return 'one_shot' in self._store and self._store['one_shot']

    def load(self):
        if 'config' in self._store:
            fname = self._store['config']
            try:
                if self.verbose():
                    self.log.show("INFO: Trying to open file '%s' as config\n" % fname)
                fp = open(fname, 'r')
                cfg = json.load(fp)
                if self.verbose():
                    self.log.show('INFO: Config file has been red\n')
                if isinstance(cfg, dict):
                    self._store = dict(cfg.items() + self._store.items())
                    if self.verbose():
                        self.log.show('INFO: Options has been updated from config file\n')
                    if self.debug():
                        self.log.show('DEBUG: Current options:\n')
                        self.log.dump(self._store)
                else:
                    raise ValueError
            except IOError:
                self.log.show('ERROR: Could not open config file\n')
                self.sys.die('config_not_open')
            except ValueError:
                self.log.show('ERROR: Config file is not well formed JSON\n')
                self.sys.die('config_bad')
        else:
            self.log.show('ERROR: Config filename does not specified\n')
            self.sys.die('config_not_set')

    def validate(self):
        if not (self.has('queue') and isinstance(self.get('queue'), basestring)):
            self.log.show('ERROR: queue does not specified\n')
            self.sys.die('config_bad')
        if not (self.has('action') and isinstance(self.get('action'), basestring)):
            self.log.show('ERROR: Action to run does not specified\n')
            self.sys.die('config_bad')


# Показывает подсказку по использованию программы, версию и т.д.
class Help(object):

    def __init__(self):
        self.write = sys.stdout.write

    def usage(self):
        self.write('''USAGE: rabbitworker [options]
Options:
    -h,     --help              Show this help
    -V,     --version           Show version
    -a,     --action            Select action
    -q,     --queue             Set RabbitMQ queue
    -c,     --config            Set config file
    -l,     --log               Set log file
    -v,     --verbose           Set verbose output
    -d,     --debug             Set debug output
    -1,     --one-shot          Run in one shot mode
    -s,     --strict            Die if command is not in config
    -t,     --auto-ack          Always send ACK after command execution
    -b,     --build-queue       Create RabbitMQ queue if it does not exists
''')

    def version(self):
        self.write('''Rabbitworker Version: 0.3
''')

# Выводит сообщение в лог
class Log(object):

    def __init__(self):
        self.sys = App().registry().get('sys')
        self.wout = sys.stdout.write
        self.eout = sys.stderr.write
        self.filename = None
        self.log = None
        self.flush = None

    def init(self):
        self.filename = App().registry().get('options').get(u'log_file')
        self.sys = App().registry().get('sys')
        if self.filename is None:
            self.log = sys.stderr.write
            self.flush = sys.stderr.flush
        else:
            try:
                self.logfile = open(self.filename, 'w')
                self.log = self.logfile.write
                self.flush = self.logfile.flush
            except (OSError, IOError) as e:
                self.show_err("ERROR: Can't open log file '%s'\nReason: %s" % (self.filename, e))
                self.sys.die('log_open_error')

    def show(self, string):
        try:
            self.log("%s\t%s" % (unicode(datetime.datetime.now()), string))
            self.flush()
        except (OSError, IOError) as e:
            self.show_err("ERROR: Can't write log file '%s'\nReason: %s" % (self.filename, e))
            self.sys.die('log_write_error')

    def emit(self, string):
        try:
            self.log(string)
            self.flush()
        except (OSError, IOError) as e:
            self.show_err("ERROR: Can't write log file '%s'\nReason: %s" % (self.filename, e))
            self.sys.die('log_write_error')


    def dump(self, data):
        try:
            self.log("%s\n" % pprint.pformat(data))
            self.flush()
        except (OSError, IOError) as e:
            self.show_err("ERROR: Can't write log file '%s'\nReason: %s" % (self.filename, e))
            self.sys.die('log_write_error')

    def format(self, data):
        pprint.pformat(data)

    def show_out(self, string):
        self.wout(string)

    def show_err(self, string):
        self.eout(string)

# Класс системных функций
class System(object):

    def __init__(self):
        self.ec = App().registry().get('exit_codes')

    def die(self, code_name):
        sys.exit(self.ec[code_name])


# Обработчик опций
class OptionDispatcher(object):

    def __init__(self, argv):
        self.help = App().registry().get('help')
        self.sys = App().registry().get('sys')
        self.options = App().registry().get('options')
        self.argv = argv

    def run(self):
        try:
            opts, args = getopt.getopt(self.argv[1:], 'Vhvc:dskq:a:1l:tbr:',
                                       ['version', 'help', 'verbose', 'config=', 'debug',
                                        'strict', 'console', 'queue=', 'action=', 'one-shot',
                                        'log=', 'auto-ack', 'build-queue','source='])
        except getopt.GetoptError:
            self.help.usage()
            self.sys.die('bad_option')
        for opt, arg in opts:
            if opt in ('-h', '--help'):
                self.help.usage()
                self.sys.die('usage')
            elif opt in ('-V', '--version'):
                self.help.version()
                self.sys.die('version')
            elif opt in ('-v', '--verbose'):
                self.options.set(u'verbose', True)
            elif opt in ('-d', '--debug'):
                self.options.set(u'debug', True)
            elif opt in ('-s', '--strict'):
                self.options.set(u'die_on_unknown_command', True)
            elif opt in ('-k', '--console'):
                self.options.set(u'command_source', u'console')
            elif opt in ('-r', '--source'):
                self.options.set(u'command_source', arg)
            elif opt in ('-c', '--config'):
                self.options.set(u'config', arg)
            elif opt in ('-q', '--queue'):
                self.options.set(u'queue', arg)
            elif opt in ('-a', '--action'):
                if isinstance(arg, basestring) and arg != '':
                    self.options.set(u'action', arg)
                else:
                    self.help.usage()
            elif opt in ('-l', '--log'):
                if isinstance(arg, basestring) and arg != '':
                    self.options.set(u'log_file', arg)
                else:
                    self.help.usage()
            elif opt in ('-1', '--one-shot'):
                self.options.set(u'one_shot', True)
            elif opt in ('-t', '--auto-ack'):
                self.options.set(u'auto_ack', True)
            elif opt in ('-b', '--build-queue'):
                self.options.set(u'build_queue', True)
            else:
                self.help.usage()
                self.sys.die('bad_option')


class Rpc(object):

    def __init__(self):
        self.log = App().registry().get('log')
        self.sys = App().registry().get('sys')
        self.options = App().registry().get('options')
        self.url = None
        self.auth_user = None
        self.auth_pass = None
        self.auth_data = None

    def setup(self, url, user, password):
        self.auth_user = user
        self.auth_pass = password
        self.url = url

    # Инициализировать параметрами из экшена
    def init(self, params):
        if self.validate(params):
            self.setup(params[u'url'], params[u'user'], params[u'pass'])
        else:
            self.log.show('ERROR: Bad rpc parameters:\n')
            self.log.dump(params)
            self.sys.die('bad_config')

    def validate(self, params):
        if isinstance(params, dict) and u'url' in params and u'user' in params and u'pass' in params:
            return True
        else:
            return False

    def auth(self):
        pass

    def single_auth(self):
        pass

    def get_response(self, data):
        if not isinstance(data, dict) or u'method' not in data:
            self.log.show('ERROR: RPC request method does not specified\n')
            self.sys.die('rpc_no_method')
        self.auth()


# Класс запросов JSON RPC
class JsonRpc(Rpc):

    def __init__(self):
        super(JsonRpc, self).__init__()

    def init(self, params):
        super(JsonRpc, self).init(params)

    def setup(self, url, user, password):
        super(JsonRpc, self).setup(url, user, password)
        if self.options.debug():
            self.log.show("DEBUG: JSON RPC initialized for url '%s' user '%s' password '%s'\n" %(self.url, self.auth_user, self.auth_pass))

    def single_auth(self):
        try:
            self.auth_data = HTTPBasicAuth(self.auth_user, self.auth_pass)
            if self.options.debug():
                self.log.show("DEBUG: JSON RPC authenticated for user '%s' password '%s'\n" %(self.auth_user, self.auth_pass))
        except Exception as e:
            self.log.show("ERROR: JSON RPC authentication failed. Reason:\n%s\n" % e)
            self.sys.die('json_rpc_failed')

    def validate(self, params):
        return super(JsonRpc, self).validate(params)

    def get_response(self, data):
        if self.options.debug():
            self.log.show("DEBUG: JSON RPC request:\n")
            self.log.dump(data)
        super(JsonRpc, self).get_response(data)
        try:
            r = requests.post(self.url, data=json.dumps(data), headers={'content-type':'application/json'}, auth=self.auth_data)
        except requests.exceptions.ConnectionError as e:
            self.log.show(u"ERROR: JSON RPC request failed. Reason: %s\n" % unicode(e))
            self.sys.die('rpc_io_error')

        response = r.json()
        if self.options.debug():
            self.log.show("DEBUG: JSON RPC response:\n")
            self.log.dump(response)
        return response


# Класс запросов SOAP
class Soap(Rpc):

    def __init__(self):
        super(Soap, self).__init__()
        self.client = None

    def init(self, params):
        super(Soap, self).init(params)

    def setup(self, url, user, password):
        super(Soap, self).setup(url, user, password)
        try:
            self.client = suds.client.Client(self.url)
            if self.options.debug():
                self.log.show("DEBUG: SOAP initialized for url '%s' user '%s' password '%s'\n" %(self.url, self.auth_user, self.auth_pass))
        except Exception as e:
            self.log.show("ERROR: SOAP initialization failed. Reason:\n%s\n" % e)
            self.sys.die('soap_failed')


    def auth(self):
        pass # не нужно каждый раз, только в setup?

    def single_auth(self):
        # FIXME Получить identity
        try:
            self.auth_data = self.client.service.Authorization('identity', self.auth_user, self.auth_pass)
            if (self.auth_data.Status<0):
                self.log.show("ERROR: SOAP authentication failed. Reason:\n%s\n" % self.auth_data.Description)
                self.sys.die('soap_badauth')
            if self.options.debug():
                self.log.show("DEBUG: SOAP authenticated for user '%s' password '%s'\n" %(self.auth_user, self.auth_pass))
        except Exception as e:
            self.log.show("ERROR: SOAP authentication failed. Reason:\n%s\n" % e)
            self.sys.die('soap_failed')


    def validate(self, params):
        return super(Soap, self).validate(params)

    def get_response(self, data):
        if self.options.debug():
            self.log.show("DEBUG: SOAP request:\n")
            self.log.dump(data)
        super(Soap, self).get_response(data)
        method = data[u'method']
        del data[u'method']
        try:
            response = self.client.service['RtsWebServiceSoap'][method](data)
            if self.options.debug():
                self.log.show("DEBUG: SOAP response:\n")
                self.log.dump(response)
            return response
        except KeyboardInterrupt:
            raise
        except Exception as e:
            self.log.show("ERROR: SOAP failed. Reason:\n%s\n" % e)
            self.sys.die('soap_failed')


# Базовый класс действия
class Action(object):

    def __init__(self):
        self.log = App().registry().get('log')
        self.sys = App().registry().get('sys')
        self.options = App().registry().get('options')
        self.name = None
        self.description = {}
        self.type = None
        self.params = {}
        self.delay = None

    def init(self, name, description):
        self.name = name
        self.description = description
        if self.validate():
            if self.options.verbose():
                self.log.show("INFO: Action '%s' has passed validation\n" % self.name)
        else:
            self.log.show("ERROR: Action '%s' has not passed validation\n" % self.name)
            self.sys.die('action_invalid')

    def validate(self):
        if self.options.debug():
            self.log.show("DEBUG: Action '%s' is base validating\n" % self.name)
        if Action.correct(self.description):
            self.type = self.description[u'type']
            self.params = self.description[u'params']
            if (u'delay' in self.description and
                (isinstance(self.description[u'delay'],int) or
                isinstance(self.description[u'delay'],float))):
                    self.delay = self.description[u'delay']
            return True
        else:
            return False

    def run(self, i):
        self.log.show("ERROR: Abstract action '%s' has been run\n" % self.name)
        self.sys.die('action_abstract_run')

    def __repr__(self):
        return "ActionClass: name '%s' type '%s' params '%s'" % (self.name, self.type, self.params)


    @staticmethod
    def correct(description):
        return u'type' in description and u'params' in description

    def new(self):
        return Action()

    @staticmethod
    def make(name, description):
        if Action.correct(description):
            t = description[u'type']
            acts = App().registry().get('actions_store')
            if t in acts:
                act = acts[t].new()
                act.init(name, description)
                return act
            else:
                App().registry().get('log').show("ERROR: Action '%s' has unknown type '%s'\n" %
                                                 (name, description[u'type']))
                App().registry().get('sys').die('action_unknown')

    def wait(self):
        if (self.delay is not None
            and not self.options.get(u'one_shot')
            and not self.options.get(u'auto_ack')):
                if self.options.verbose():
                    self.log.show('INFO: Sleep for %g second(s)\n' % self.delay)
                time.sleep(self.delay)



# Действие - запуск внешней программы
class ExecAction(Action):

    def __init__(self):
        self.cmd = None
        self.args = []
        self.popen_args = None
        self.input = None
        super(ExecAction, self).__init__()

    def init(self, name, description):
        super(ExecAction, self).init(name, description)

    def new(self):
        return ExecAction()

    def validate(self):
        if self.options.debug():
            self.log.show("DEBUG: Action '%s' is exec validating\n" % self.name)
        if (super(ExecAction, self).validate() and u'exec' in self.params and
                isinstance(self.params[u'exec'], unicode)):
            self.cmd = self.params[u'exec']
            if u'args' in self.params and isinstance(self.params[u'args'], list):
                self.args = self.params[u'args']
            self.popen_args = [self.cmd] + self.args
            if u'input' in self.params:
                self.input = self.params[u'input']
            return True
        else:
            return False

    def run(self, i):
        fout = None
        ferr = None
        pid = None
        if i is None:
            i = ''
        i = (self.input if self.input is not None else '') + i
        try:
            if self.options.verbose():
                self.log.show("INFO: Run action '%s'\n" % self.name)
            process = subprocess.Popen(self.popen_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
            pid = process.pid
            if self.options.verbose():
                self.log.show("INFO: Action PID: %d\n" % pid)

            fout, ferr = process.communicate(i)
            self.log.show_out(str(fout))
            self.log.show_err(str(ferr))

            if self.options.verbose():
                self.log.show('INFO: Return code: %d\n' % process.returncode)

            if process.returncode:
                self.wait()

            return process.returncode
        except OSError as e:
            self.log.show("WARNING: Action '%s' is terminated abnormally\nERROR: Reason: %s\n" % (self.name, e))
            self.wait()
#            App().registry().get('sys').die('action_unknown')


# Действие - публикация на РТС через SOAP
class RtsAction(Action):

    def __init__(self):
        self.jsonrpc_params = None
        self.soap_params = None
        super(RtsAction, self).__init__()
        self.jsonrpc = App().registry().get('json-rpc')
        self.soap = App().registry().get('soap')

    def init(self, name, description):
        super(RtsAction, self).init(name, description)

    def new(self):
        return RtsAction()

    def validate(self):
        if self.options.debug():
            self.log.show("DEBUG: Action '%s' is rts validating\n" % self.name)
        if (super(RtsAction, self).validate()
                and u'json-rpc' in self.params
                and isinstance(self.params[u'json-rpc'], dict)
                and u'soap' in self.params
                and isinstance(self.params[u'soap'], dict)):
            self.jsonrpc_params = self.params[u'json-rpc']
            self.soap_params = self.params[u'soap']
            self.jsonrpc.init(self.jsonrpc_params)
            self.soap.init(self.soap_params)
            return True
        else:
            return False

    def run(self, i):
        if self.options.verbose():
            self.log.show("INFO: Run action '%s'\n" % self.name)

        if self.options.debug():
            self.log.show("DEBUG: Action input '%s'\n" % unicode(i))

        try:
            # Запросить номер аукциона
            self.jsonrpc.single_auth();
            jr = self.jsonrpc.get_response({u'jsonrpc': u'2.0', u'method': u'ext.лот', u'id': 1, u'params': [i]})
            rts_id = int(jr[u'result'][u'госзакупки_id'])
            # SOAP запрос
            self.soap.single_auth()
            sr = self.soap.get_response({'method': 'BindAuctionToIntegrationPlatform', 'uniqueToken': '???token???', 'auctionNumber': rts_id, 'platformId': '???plId???'})
            if sr.Status >= 0:
                if self.options.verbose():
                    self.log.show("INFO: Success '%s'\n" % sr.Description)
            else:
                if self.options.verbose():
                    self.log.show("INFO: Fail '%s'\n" % sr.Description)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            self.log.show("WARNING: Action '%s' is terminated abnormally\nERROR: Reason: %s\n" % (self.name, e))
            self.wait()

# Хранилище действий
class Actions(object):

    def __init__(self):
        self.log = App().registry().get('log')
        self.sys = App().registry().get('sys')
        self.options = None
        self.actions = {}

    def init(self):
        self.options = App().registry().get('options')
        if self.options.has(u'actions'):
            actions = self.options.get(u'actions') # Список экшенов в конфиге
            for k, v in actions.items():
                self.actions[k] = Action.make(k, v)
            if self.options.debug():
                self.log.show('DEBUG: Actions recognized:\n')
                self.log.dump(self.actions)
        else:
            self.log.show('ERROR: Config has no actions')
            self.sys.die('config_no_actions')

    def names(self):
        return self.actions.keys()

    def run(self, name, i):
        return self.actions[name].run(i)


# Класс диспетчера действий
class CommandRunner(object):

    def __init__(self):
        self.log = App().registry().get('log')
        self.sys = App().registry().get('sys')
        self.options = App().registry().get('options')
        self.actions = App().registry().get('actions')
        self.command = self.options.get(u'action')

    def run(self, i):
        if self.command in self.actions.names():
            return self.actions.run(self.command, i)
        else:
            if self.options.die_on_unknown_command():
                self.log.show("ERROR: Command %s is not recognized\n" % self.command)
                self.sys.die('command_unknown')
            else:
                self.log.show("WARNING: Command %s is not recognized. Skipped\n" % self.command)


# Базовый класс источника комманд
class CommandSource(object):

    def __init__(self):
        self.log = App().registry().get('log')
        self.sys = App().registry().get('sys')
        self.runner = App().registry().get('command_runner')

    def init(self):
        self.options = App().registry().get('options')

    def run(self):
        self.log.show("ERROR: Abstract command source has been invoked\n")
        self.sys.die('action_abstract_run')

    @staticmethod
    def make(source):
        sources = App().registry().get('command_sources')

        if source in sources:
            return sources[source]
        else:
            App().registry().get('log').show("ERROR: Unknown command source '%s' requested" % source)
            App().registry().get('sys').die('source_unknown')


# Класс ввода комманд из консоли
class RawInputCommandSource(CommandSource):

    def run(self):
        try:
            while True:
                i = raw_input('INPUT: stdin >')
                self.runner.run(i)
        except (KeyboardInterrupt, EOFError):
            self.log.show("\nINFO: End of input\n")
            self.sys.die('ok')


# Класс получения комманд через RabbitMQ
class RabbitMQCommandSource(CommandSource):

    def __init__(self):
        super(RabbitMQCommandSource, self).__init__()
        self.connection = None
        self.chan = None

    def init(self):
        super(RabbitMQCommandSource, self).init()

        connection_parms = {}
        # Установить аутентификацию RabbitMQ
        if self.options.has('host'):
            connection_parms['host'] = self.options.get(u'host')
        if self.options.has('port'):
            connection_parms['port'] = self.options.get(u'port')
        if self.options.has('password'):
            connection_parms['credentials'] = pika.PlainCredentials(self.options.get(u'username'), self.options.get(u'password'))
        if self.options.has('ssl'):
            connection_parms['ssl'] = self.options.get(u'ssl')
        if self.options.has('ssl_options'):
            connection_parms['ssl_options'] = self.options.get(u'ssl_options')
        if self.options.has('locale'):
            connection_parms['locale'] = self.options.get(u'locale')
        if self.options.has('connection_attempts'):
            connection_parms['connection_attempts'] = self.options.get(u'connection_attempts')

            # Установить соединение с RabbitMQ
        if self.options.verbose():
            self.log.show('INFO: Open connection to RabbitMQ\n')
        if self.options.debug():
            self.log.show('DEBUG: RabbitMQ connection parameters:\n')
            self.log.dump(connection_parms)
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(**connection_parms))
            self.chan = self.connection.channel()
        except pika.exceptions.AMQPError as e:
            self.log.show("ERROR: Can't connect to RabbitMQ. Reason: %s\n" % e)
            self.sys.die('amqp_io_error')

        queue_parms = {}
        if self.options.has('queue'):
            queue_parms['queue'] = self.options.get(u'queue')
        else:
            self.log.show('ERROR: RabbitMQ queue name does not specified\n')
            self.sys.die('amqp_no_queue')
        if self.options.has('durable'):
            queue_parms['durable'] = self.options.get(u'durable')
        if self.options.has('exclusive'):
            queue_parms['exclusive'] = self.options.get(u'exclusive')

        if self.options.verbose():
            self.log.show("INFO: Binding to RabbitMQ queue'\n")
        if self.options.debug():
            self.log.show('DEBUG: RabbitMQ queue_declare parameters:\n')
            self.log.dump(queue_parms)
        try:
            if self.options.get(u'build_queue'):
                result = self.chan.queue_declare(**queue_parms)
                queue_name = result.method.queue
            else:
                queue_name = queue_parms['queue']
            if self.options.verbose():
                self.log.show("INFO: Start listening RabbitMQ queue: '%s'\n" % queue_name)
            self.chan.basic_consume(self.on_receive, queue=queue_name, no_ack=False)
        except pika.exceptions.AMQPError as e:
            self.log.show("ERROR: Can't start RabbitMQ message receiving. Reason: %s\n" % e)
            self.sys.die('amqp_rec_error')

    # Обработкчик сообщений
    def on_receive(self, channel, method, props, body):
        if self.options.verbose():
            self.log.show('INFO: RabbitMQ message received\n')
            if props.correlation_id and isinstance(props.correlation_id, int):
                self.log.show('INFO: correlation_id: %d\n' % props.correlation_id)
            else:
                self.log.show('INFO: correlation_id: None\n')
        if self.options.debug():
            self.log.show('DEBUG: Message body:\n')
            self.log.dump(body)
        if isinstance(body, basestring):
            if self.runner.run(body) == 0:
                if self.options.verbose():
                    self.log.show('INFO: Action executed successfully. Sending ACK\n')
                channel.basic_ack(delivery_tag=method.delivery_tag)

                if self.options.one_shot():
                    if self.options.verbose():
                        self.log.show('INFO: Action processed. Exit\n')
                    self.sys.die('ok')
            else:
                if self.options.get(u'auto_ack'):
                    if self.options.verbose():
                        self.log.show('INFO: Action does not executed successfully. Sending ACK\n')
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                else:
                    if self.options.verbose():
                        self.log.show('INFO: Action does not executed successfully. Sending NACK\n')
                    channel.basic_nack(delivery_tag=method.delivery_tag)
            if self.options.one_shot():
                if self.options.verbose():
                    self.log.show('INFO: One shot action. Exit\n')
                self.sys.die('ok')
        else:
            self.log.show('ERROR: Message is not a string!\n')
            self.sys.die('message_bad')


    # Запустить приёмник
    def run(self):
        try:
            self.chan.start_consuming()
        except pika.exceptions.AMQPError as e:
            self.log.show("ERROR: RabbitMQ consumer crash\nReason: %s\n" % e)
            self.sys.die('amqp_rec_error')
        except (KeyboardInterrupt, EOFError):
            if self.options.verbose():
                self.log.show("INFO: Program interrupted\n")
            self.sys.die('ok')
        finally:
            self.chan.stop_consuming()
            self.connection.close()

# Класс приложения
class App (object):
    _reg = Registry()

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(App, cls).__new__(cls)
        return cls.instance

    # Отдать репозиторий объектов
    def registry(self):
        return self._reg

    # Заполнить репозиторий объектов
    def __init__(self):
        pass

    def init(self):
        # Коды выхода
        self.registry().set('exit_codes', {
            'ok': 0,            # Нормальный выход
            'failure': -1,      # Ошибка без уточнения
            'usage': 1,         # Запрос подсказки
            'version': 2,       # Запрос версии
            'bad_option': 3,    # Неверная опция
            'config_not_set': 4,
            'config_not_open': 5,
            'config_bad': 6,
            'action_abstract_run': 7,
            'action_invalid': 8,
            'config_no_actions': 9,
            'action_unknown': 10,
            'command_unknown': 11,
            'amqp_io_error': 12,
            'amqp_rec_error': 13,
            'message_bad': 14,
            'log_open_error': 15,
            'log_write_error': 16,
            'amqp_no_queue': 17,
            'json_rpc_noauth': 18,
            'json_rpc_badauth': 19,
            'soap_noauth': 20,
            'soap_badauth': 21,
            'soap_failed': 22,
            'rpc_no_method': 23,
            'rpc_io_error': 24
        })
        # Системные функции
        self.registry().set('sys', System())
        # Вывод подсказки по опциям
        self.registry().set('help', Help())
        # Вывод лога
        self.registry().set('log', Log())
        # Хранилище опций + опции по умолчанию
        options = Options()
        options.set(u'config', 'rabbitworker.json')
        options.set(u'log_file', None)
        options.set(u'debug', False)
        options.set(u'verbose', False)
        options.set(u'auto_ack', False)
        options.set(u'one_shot', False)
        options.set(u'build_queue', False)
        options.set(u'command_source', u'rabbitmq')
        self.registry().set('options', options)
        self.registry().set('option_dispatcher', OptionDispatcher(sys.argv))
        self.registry().set('json-rpc', JsonRpc())
        self.registry().set('soap', Soap())
        self.registry().set('actions', Actions())
        # Типы событий
        self.registry().set('actions_store', {
            'exec': ExecAction(),
            'rts': RtsAction()
        })
        # Обработать опции коммандной строки
        self.registry().get('option_dispatcher').run()
        self.registry().get('log').init()

        # Загрузить опции из файла
        options = self.registry().get('options')
        options.load()
        options.validate()
        # Настроить события
        self.registry().get('actions').init()
        # Установить исполнителя комманд
        self.registry().set('command_runner', CommandRunner())
        # Настроить источники комманд
        self.registry().set('command_sources', {
            'console': RawInputCommandSource(),
            'rabbitmq': RabbitMQCommandSource()
        })
        # Выбрать источник комманд
        self.registry().set('command_source', CommandSource.make(options.get(u'command_source')))
        # Сигнал для завершения
        signal.signal(signal.SIGUSR1, App.on_exit)

    def run(self):
        try:
            cs = self.registry().get('command_source')
            cs.init()
            cs.run()
        except (KeyboardInterrupt, EOFError):
            self.registry().get('sys').die('ok')

    @staticmethod
    def on_exit(num, stack):
        r = App().registry()
        if r.get('options').verbose():
            r.get('log').show('INFO: Got SIGUSR1. Exit\n')
        r.get('sys').die('ok')


# Запуск приложения
if __name__ == '__main__':
    App().init()
    App().run()
