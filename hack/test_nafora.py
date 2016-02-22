import unittest2 as unittest

from testcase import CassandraTestcase

class Testcase_abc(CassandraTestcase):

    def test_connect(self):
        print self.session
        print self.cluster

class TestSet(CassandraTestcase):

    def test_will_work(self):
        pass

    def will_not_work(self):
        pass