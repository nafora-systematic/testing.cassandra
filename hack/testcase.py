# -*- coding: utf-8 -*-
#  Copyright 2013 Takeshi KOMIYA
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
import sys
import signal
import socket
import tempfile
import subprocess
from time import sleep
from shutil import copytree, rmtree, copyfile
from datetime import datetime

import re
import yaml
import pycassa
from glob import glob



class DatabaseFactory(object):
    target_class = None

    def __init__(self, **kwargs):
        self.cache = None
        self.settings = kwargs

        init_handler = self.settings.pop('on_initialized', None)
        if self.settings.pop('cache_initialized_db', None):
            if init_handler:
                try:
                    self.cache = self.target_class()
                    init_handler(self.cache)
                except:
                    if self.cache:
                        self.cache.stop()
                    raise
                finally:
                    if self.cache:
                        self.cache.terminate()
            else:
                self.cache = self.target_class(auto_start=0)
                self.cache.setup()
            self.settings['copy_data_from'] = self.cache.get_data_directory()

    def __call__(self):
        return self.target_class(**self.settings)

    def clear_cache(self):
        if self.cache:
            self.settings['copy_data_from'] = None
            self.cache.cleanup()


class Database(object):
    BOOT_TIMEOUT = 10.0
    DEFAULT_SETTINGS = {}
    subdirectories = []

    def __init__(self, **kwargs):
        self.name = self.__class__.__name__
        self.settings = dict(self.DEFAULT_SETTINGS)
        self.settings.update(kwargs)
        self.child_process = None
        self._owner_pid = os.getpid()
        self._use_tmpdir = False

        self.base_dir = self.settings.pop('base_dir')
        if self.base_dir:
            if self.base_dir[0] != '/':
                self.base_dir = os.path.join(os.getcwd(), self.base_dir)
        else:
            self.base_dir = tempfile.mkdtemp()
            self._use_tmpdir = True

        try:
            self.initialize()

            if self.settings['auto_start']:
                if self.settings['auto_start'] >= 2:
                    self.setup()

                self.start()
        except:
            self.cleanup()
            raise

    def initialize(self):
        pass

    def setup(self):
        # copy data files
        if self.settings['copy_data_from']:
            try:
                data_dir = self.get_data_directory()
                copytree(self.settings['copy_data_from'], data_dir)
                os.chmod(data_dir, 0o700)
            except Exception as exc:
                raise RuntimeError("could not copytree %s to %s: %r" %
                                   (self.settings['copy_data_from'], data_dir, exc))

        # create directory tree
        for subdir in self.subdirectories:
            path = os.path.join(self.base_dir, subdir)
            if not os.path.exists(path):
                os.makedirs(path)
                os.chmod(path, 0o700)

        try:
            self.initialize_database()
        except:
            self.cleanup()
            raise

    def get_data_directory(self):
        pass

    def initialize_database(self):
        pass

    def start(self):
        if self.child_process:
            return  # already started

        self.prestart()

        logger = open(os.path.join(self.base_dir, '%s.log' % self.name), 'wt')
        try:
            command = self.get_server_commandline()
            self.child_process = subprocess.Popen(command, stdout=logger, stderr=logger)
        except Exception as exc:
            raise RuntimeError('failed to launch %s: %r' % (self.name, exc))
        else:
            try:
                self.wait_booting()
                self.poststart()
            except:
                self.stop()
                raise
        finally:
            logger.close()

    def get_server_commandline(self):
        raise NotImplemented

    def wait_booting(self):
        exec_at = datetime.now()
        while True:
            if self.child_process.poll() is not None:
                raise RuntimeError("*** failed to launch %s ***\n" % self.name +
                                   self.read_bootlog())

            if self.is_server_available():
                break

            if (datetime.now() - exec_at).seconds > self.BOOT_TIMEOUT:
                raise RuntimeError("*** failed to launch %s (timeout) ***\n" % self.name +
                                   self.read_bootlog())

            sleep(0.1)

    def prestart(self):
        if self.settings['port'] is None:
            self.settings['port'] = get_unused_port()

    def poststart(self):
        pass

    def is_server_available(self):
        return False

    def is_alive(self):
        return self.child_process and self.child_process.poll() is None

    @property
    def server_pid(self):
        return getattr(self.child_process, 'pid', None)

    def stop(self, _signal=signal.SIGTERM):
        try:
            self.terminate(_signal)
        finally:
            self.cleanup()

    def terminate(self, _signal=signal.SIGTERM):
        if self.child_process is None:
            return  # not started

        if self._owner_pid != os.getpid():
            return  # could not stop in child process

        try:
            self.child_process.send_signal(_signal)
            killed_at = datetime.now()
            while self.child_process.poll() is None:
                if (datetime.now() - killed_at).seconds > 10.0:
                    os.child_process.kill()
                    raise RuntimeError("*** failed to shutdown postgres (timeout) ***\n" + self.read_bootlog())

                sleep(0.1)
        except OSError:
            pass

        self.child_process = None

    def cleanup(self):
        if self.child_process is not None:
            return

        if self._use_tmpdir and os.path.exists(self.base_dir):
            rmtree(self.base_dir, ignore_errors=True)
            self._use_tmpdir = False

    def read_bootlog(self):
        try:
            with open(os.path.join(self.base_dir, '%s.log' % self.name)) as log:
                return log.read()
        except Exception as exc:
            raise RuntimeError("failed to open file:%s.log: %r" % (self.name, exc))

    def __del__(self):
        self.stop()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.stop()


class SkipIfNotInstalledDecorator(object):
    name = ''

    def search_server(self):
        pass  # raise exception if not found

    def __call__(self, arg=None):
        if sys.version_info < (2, 7):
            from unittest2 import skipIf
        else:
            from unittest import skipIf

        def decorator(fn, path=arg):
            if path:
                cond = not os.path.exists(path)
            else:
                try:
                    self.search_server()
                    cond = False  # found
                except:
                    cond = True  # not found

            return skipIf(cond, "%s not found" % self.name)(fn)

        if callable(arg):  # execute as simple decorator
            return decorator(arg, None)
        else:  # execute with path argument
            return decorator


def get_unused_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('localhost', 0))
    _, port = sock.getsockname()
    sock.close()

    return port


def get_path_of(name):
    path = subprocess.Popen(['/usr/bin/which', name],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE).communicate()[0]
    if path:
        return path.rstrip().decode('utf-8')
    else:
        return None


SEARCH_PATHS = ['/usr/local/cassandra',
                '/usr/local/apache-cassandra',
                '/usr/local/opt/cassandra']


class Cassandra(Database):
    BOOT_TIMEOUT = 20
    DEFAULT_SETTINGS = dict(auto_start=2,
                            base_dir=None,
                            cassandra_home=None,
                            pid=None,
                            copy_data_from=None)
    subdirectories = ['conf', 'commitlog', 'data', 'saved_caches', 'tmp']

    def initialize(self, **kwargs):
        self.cassandra_home = self.settings.get('cassandra_home')
        if self.cassandra_home is None:
            self.cassandra_home = find_cassandra_home()

        self.cassandra_bin = self.settings.get('cassandra_bin')
        if self.cassandra_bin is None:
            self.cassandra_bin = os.path.join(self.cassandra_home, 'bin', 'cassandra')

        with open(os.path.join(self.cassandra_confdir, 'cassandra.yaml')) as fd:
            self.cassandra_yaml = yaml.load(fd.read())
            self.cassandra_yaml['commitlog_directory'] = os.path.join(self.base_dir, 'commitlog')
            self.cassandra_yaml['data_file_directories'] = [os.path.join(self.base_dir, 'data')]
            self.cassandra_yaml['saved_caches_directory'] = os.path.join(self.base_dir, 'saved_caches')
            self.cassandra_yaml['start_native_transport'] =  "true"
            self.cassandra_yaml['listen_address'] =  "127.0.0.1"


            cassandra_version = strip_version(self.cassandra_home)
            if cassandra_version is None or cassandra_version > (1, 2):
                self.cassandra_yaml['start_rpc'] = True

            for key, value in self.settings.get('cassandra_yaml', {}):
                self.settings['cassandra_yaml'][key] = value

        if self.settings['auto_start']:
            if os.path.exists(self.pid_file):
                raise RuntimeError('cassandra is already running (%s)' % self.pid_file)

    @property
    def pid_file(self):
        return os.path.join(self.base_dir, 'tmp', 'cassandra.pid')

    @property
    def cassandra_confdir(self):
        path = os.path.join(self.cassandra_home, 'conf')
        if os.path.exists(path):
            return path
        elif os.path.exists('/usr/local/etc/cassandra'):  # Homebrew
            return '/usr/local/etc/cassandra'
        else:
            raise RuntimeError("could not find confdir of cassandra")

    def server_list(self):
        hostname = '127.0.0.1:%d' % self.cassandra_yaml['rpc_port']
        return [hostname]

    def get_data_directory(self):
        return os.path.join(self.base_dir, 'data')

    def initialize_database(self):
        # conf directory
        conf_dir = os.path.join(self.base_dir, 'conf')
        for filename in os.listdir(self.cassandra_confdir):
            srcpath = os.path.join(self.cassandra_confdir, filename)
            destpath = os.path.join(conf_dir, filename)
            if not os.path.exists(destpath):
                if filename == 'log4j-server.properties':
                    logpath = os.path.join(self.base_dir, 'tmp', 'system.log')
                    with open(srcpath) as src:
                        with open(destpath, 'w') as dest:
                            property = re.sub('log4j.appender.R.File=.*',
                                              'log4j.appender.R.File=%s' % logpath,
                                              src.read())
                            dest.write(property)
                elif os.path.isdir(srcpath):
                    copytree(srcpath, destpath)
                else:
                    copyfile(srcpath, destpath)

    def get_server_commandline(self):
        return [self.cassandra_bin, '-f']

    def is_server_available(self):
        try:
            sock = socket.create_connection(('127.0.0.1', self.cassandra_yaml['rpc_port']), 1.0)
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
            return True
        except:
            return False

    def prestart(self):
        os.environ['CASSANDRA_CONF'] = os.path.join(self.base_dir, 'conf')

        # assign ports to cassandra
        config_keys = ['rpc_port', 'storage_port', 'ssl_storage_port', 'native_transport_port']
        for key in config_keys:
            if key in self.cassandra_yaml:
                self.cassandra_yaml[key] = get_unused_port()

        # replace cassandra-env.sh
        with open(os.path.join(self.base_dir, 'conf', 'cassandra-env.sh'), 'r+t') as fd:
            script = re.sub('JMX_PORT="7199"', 'JMX_PORT="%d"' % get_unused_port(), fd.read())
            fd.seek(0)
            fd.write(script)

        # generate cassandra.yaml
        with open(os.path.join(self.base_dir, 'conf', 'cassandra.yaml'), 'wt') as fd:
            fd.write(yaml.dump(self.cassandra_yaml))

    def poststart(self):
        # create test keyspace
        conn = pycassa.system_manager.SystemManager(self.server_list()[0])
        try:
            conn.create_keyspace('test', pycassa.SIMPLE_STRATEGY, {'replication_factor': '1'})
        except pycassa.InvalidRequestException:
            pass
        conn.close()


class CassandraSkipIfNotInstalledDecorator(SkipIfNotInstalledDecorator):
    name = 'Cassandra'

    def search_server(self):
        find_cassandra_home()  # raise exception if not found


skipIfNotFound = skipIfNotInstalled = CassandraSkipIfNotInstalledDecorator()


def strip_version(dir):
    m = re.search('(\d+)\.(\d+)\.(\d+)', dir)
    if m is None:
        return None
    else:
        return tuple([int(ver) for ver in m.groups()])


def find_cassandra_home():
    cassandra_home = os.environ.get('CASSANDRA_HOME')
    if cassandra_home and os.path.exists(os.path.join(cassandra_home, 'bin', 'cassandra')):
        return cassandra_home

    for dir in SEARCH_PATHS:
        if os.path.exists(os.path.join(dir, 'bin', 'cassandra')):
            return dir

    # search newest cassandra-x.x.x directory
    cassandra_dirs = [dir for dir in glob("/usr/local/*cassandra*") if os.path.isdir(dir)]
    if cassandra_dirs:
        return sorted(cassandra_dirs, key=strip_version)[-1]

    raise RuntimeError("could not find CASSANDRA_HOME")

import unittest2 as unittest
from cassandra.cluster import Cluster
import pycassa

class CassandraTestcase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(CassandraTestcase, self).__init__(*args, **kwargs)
        copy_data_from=None
        cassandra_yaml= {"start_native_transport": True}
        self.copy_data_from = copy_data_from
        self.cassandra_yaml = cassandra_yaml

    def setUp(self):
        self.cassandra = Cassandra()#(, copy_data_from = self.copy_data_from)

        server = self.cassandra.server_list()
        print "Connecting to Cassandra at %s" % server

        #PyCassa - this works
        #self.conn = pycassa.pool.ConnectionPool('test', server)

        #Cassandra
        self.cluster = Cluster(server) # works
        #self.session = self.cluster.connect() # fails

    def tearDown(self):
        self.cassandra.stop()
