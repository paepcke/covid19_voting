'''
Created on Sep 27, 2020

@author: paepcke
'''
import os
import unittest

from mail_voting_transformer import MailVotingTransformer


#*****TEST_ALL = True
TEST_ALL = False


class MailVotingTest(unittest.TestCase):

    DATA_DIR = os.path.join(os.path.dirname(__file__), '../../data')

    #------------------------------------
    # setUP
    #-------------------

    def setUp(self):
        pass

    #------------------------------------
    # tearDown 
    #-------------------

    def tearDown(self):
        pass

    #------------------------------------
    # test_2018_intake
    #-------------------

    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_2018_intake(self):
        
        xformer = MailVotingTransformer({2018 : os.path.join(self.DATA_DIR,
                                                             'voteByMail2018.xlsx')
                                         })
        print('foo')
        
    #------------------------------------
    # test_2016_intake 
    #-------------------

    #****@unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_2016_intake(self):
        xformer = MailVotingTransformer({2016 : os.path.join(self.DATA_DIR,
                                                             'voteByMail2016.xlsx')
                                         })
        print('foo')


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testIntake']
    unittest.main()