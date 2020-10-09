'''
Created on Oct 1, 2020

@author: paepcke
'''

import unittest

import pandas as pd

from eavs_cleaning import ElectionSurveyCleaner

pd.set_option('display.max_columns', None)  
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', None)


#****TEST_ALL = True
TEST_ALL = False


class MailVotingTest(unittest.TestCase):

    #------------------------------------
    # setUpClass 
    #-------------------
    
    @classmethod
    def setUpClass(cls):
        cls.EAVS_FILES = ElectionSurveyCleaner.EAVS_FILES


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
    # test_2018
    #-------------------

    #****@unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_2018(self):
        year = 2018
        df = ElectionSurveyCleaner().transform(year)
        self.assertEqual(df.index[0], ('0100100000', 'AL', 'AUTAUGA COUNTY', year))
        durham_vote_count = df.reindex(['DURHAM COUNTY'], 
                                       level='Jurisdiction')[f'{year}ByMailCountBallotsReturned'].item()
        self.assertEqual(int(durham_vote_count), 2420)
        # All sums should be positive, lest we missed 
        # a -888888 or -999999 code (Data not Applicable/Available)
        self.assertTrue(all(df[f'{year}ByMailCountBallotsReturned'] >= 0))
        
        # Should have no nan in County FIPS cols:
        self.assertEqual(df[f'FIPSCoarse{year}'].isna().sum(), 0)
        # Same with State abbreviation and Jurisdiction in the multiindex:
        self.assertEqual(df.index.get_level_values('State').isna().sum(), 0)
        self.assertEqual(df.index.get_level_values('Jurisdiction').isna().sum(), 0)

    #------------------------------------
    # test_2016
    #-------------------

    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_2016(self):
        year = 2016
        df = ElectionSurveyCleaner().transform(year)
        
        self.assertEqual(df.index[0], ('0200000000', 'AK', 'ALASKA', year))
        spotcheck = df.loc[('2300102060','ME','AUBURN')][f'{year}TotalVote'].values[0]
        self.assertEqual(spotcheck, 11986)
        # All sums should be positive, lest we missed 
        # a -888888 or -999999 code (Data not Applicable/Available)
        self.assertTrue(all(df[f'{year}TotalVote'] >= 0))
        
        # Should have no nan in County FIPS cols:
        self.assertEqual(df[f'FIPSCoarse{year}'].isna().sum(), 0)
        # Same with State abbreviation and Jurisdiction in the multiindex:
        self.assertEqual(df.index.get_level_values('State').isna().sum(), 0)
        self.assertEqual(df.index.get_level_values('Jurisdiction').isna().sum(), 0)

    #------------------------------------
    # test_2014
    #-------------------

    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_2014(self):
        year = 2014
        df = ElectionSurveyCleaner().transform(year)
        self.assertEqual(df.index[0], ('0100100000', 'AL', 'AUTAUGA COUNTY', year))
        durham_vote_count = df.reindex(['DURHAM COUNTY'], 
                                       level='Jurisdiction')[f'{year}TotalVoteProvisional'].item()
        self.assertEqual(int(durham_vote_count), 648)
        # All sums should be positive, lest we missed 
        # a -888888 or -999999 code (Data not Applicable/Available)
        self.assertTrue(all(df[f'{year}TotalVoteByMail'] >= 0))
        
        # Should have no nan in County FIPS cols:
        self.assertEqual(df[f'FIPSCoarse{year}'].isna().sum(), 0)
        # Same with State abbreviation and Jurisdiction in the multiindex:
        self.assertEqual(df.index.get_level_values('State').isna().sum(), 0)
        self.assertEqual(df.index.get_level_values('Jurisdiction').isna().sum(), 0)


# --------------------------- Main ----------
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()