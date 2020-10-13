'''
Created on Oct 1, 2020

@author: paepcke
'''

import unittest

import pandas as pd
import numpy as np

from eavs_cleaning import ElectionSurveyCleaner

pd.set_option('display.max_columns', None)  
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', None)


#******TEST_ALL = True
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

    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_2018(self):
        year = 2018
        xformer = ElectionSurveyCleaner()
        df = xformer.transform(year)
        self.assertEqual(df.index[0], ('0100100000', 'AL', 'AUTAUGA COUNTY', year))
        durham_vote_count = df.reindex(['DURHAM COUNTY'], 
                                       level='Jurisdiction')[f'{year}ByMailCountBallotsReturned'].item()
        self.assertEqual(int(durham_vote_count), 2420)
        # All sums should be positive, lest we missed 
        # a -888888 or -999999 code (Data not Applicable/Available)
        self.assertTrue(all(df[f'{year}ByMailCountBallotsReturned'] >= 0))
        
        # Should have no nan in County FIPS cols:
        self.assertEqual(df[f'{year}CountyFIPS'].isna().sum(), 0)
        # Same with State abbreviation and Jurisdiction in the multiindex:
        self.assertEqual(df.index.get_level_values('State').isna().sum(), 0)
        self.assertEqual(df.index.get_level_values('Jurisdiction').isna().sum(), 0)
        
        # Spot check one percentage: the rejection rate
        # of Barbour County in Alabama:
        percentages = xformer.percentages
        self.assertEqual(percentages.xs('AL', level='State')
                            .xs('BARBOUR COUNTY', level='Jurisdiction')['2018PercByMailRejTotal'].item(),
                            11.168831168831169)

    #------------------------------------
    # test_2016
    #-------------------

    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_2016(self):
        year = 2016
        xformer = ElectionSurveyCleaner()
        df = xformer.transform(year)
        
        self.assertEqual(df.index[0], ('0200000000', 'AK', 'ALASKA', year))
        spotcheck = df.loc[('2300102060','ME','AUBURN')][f'{year}TotalVote'].values[0]
        self.assertEqual(spotcheck, 11986)
        # All sums should be positive, lest we missed 
        # a -888888 or -999999 code (Data not Applicable/Available)
        self.assertTrue(all(df[f'{year}TotalVote'] >= 0))
        
        # Should have no nan in County FIPS cols:
        self.assertEqual(df[f'{year}CountyFIPS'].isna().sum(), 0)
        # Same with State abbreviation and Jurisdiction in the multiindex:
        self.assertEqual(df.index.get_level_values('State').isna().sum(), 0)
        self.assertEqual(df.index.get_level_values('Jurisdiction').isna().sum(), 0)

    #------------------------------------
    # test_2014
    #-------------------

    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_2014(self):
        year = 2014
        xformer = ElectionSurveyCleaner()
        df = xformer.transform(year)

        self.assertEqual(df.index[0], ('0100100000', 'AL', 'AUTAUGA COUNTY', year))
        durham_vote_count = df.reindex(['DURHAM COUNTY'], 
                                       level='Jurisdiction')[f'{year}TotalVoteProvisional'].item()
        self.assertEqual(int(durham_vote_count), 648)
        # All sums should be positive, lest we missed 
        # a -888888 or -999999 code (Data not Applicable/Available)
        self.assertTrue(all(df[f'{year}TotalVoteByMail'] >= 0))
        
        # Should have no nan in County FIPS cols:
        self.assertEqual(df[f'{year}CountyFIPS'].isna().sum(), 0)
        # Same with State abbreviation and Jurisdiction in the multiindex:
        self.assertEqual(df.index.get_level_values('State').isna().sum(), 0)
        self.assertEqual(df.index.get_level_values('Jurisdiction').isna().sum(), 0)

    #------------------------------------
    # test_percentages_2018 
    #-------------------
    
    #*****@unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_percentages_2018(self):
        year = 2018
        xformer = ElectionSurveyCleaner()
        df = xformer.transform(year)
        df_perc = xformer.percentages
        
        # Spot check: percent voted by mail. Use Sweetwater County in WY:
        
        sample_row = df.loc[('5603700000', 'WY', 'SWEETWATER COUNTY',year)]
        votes_counted = sample_row[f'{year}TotalVoteCounted']
        votes_by_mail = sample_row[f'{year}ByMailCountBallotsReturned']
        
        # The following comes to 16.201586
        perc_computed = 100 * votes_by_mail / votes_counted
        
        row = df_perc.xs('SWEETWATER COUNTY', level='Jurisdiction')
        self.assertTrue(row[f'{year}PercVoteModusByMail'].item() == perc_computed)
        
        self.assertEqual(df.xs(['WY','WESTON COUNTY'],
                               level=['State','Jurisdiction'])['2018ByMailCountBallotsSent'].item(),
                               656)
        self.assertEqual(df.xs(['WY','WESTON COUNTY'],
                               level=['State','Jurisdiction'])['2018ByMailCountBallotsReturned'].item(),
                               648)
        
        self.assertEqual(df.xs(['AL','BARBOUR COUNTY'],
                               level=['State','Jurisdiction'])['2018ByMailCountBallotsReturned'].item(),
                               770)

        self.assertEqual(df.xs(['AL','BARBOUR COUNTY'],
                               level=['State','Jurisdiction'])['2018ByMailCountByMailRejected'].item(),
                               86)
        
        rej_perc = df_perc.xs(['AL','BARBOUR COUNTY'],
                                    level=['State','Jurisdiction'])['2018PercByMailRejTotal'].item()
        self.assertEqual(round(rej_perc, 1), 11.2)
        
        # No percentages must be over 100:
        prob_col = {}
        for perc_col in df_perc.columns:
            if type(df_perc[perc_col][0]) != str:
                try:
                    self.assertEqual((df_perc[perc_col] > 100).sum(), 0)
                except AssertionError:
                    prob_col[perc_col] = [','.join(row_info[:3]) for row_info in df_perc[(df_perc[perc_col] > 100)].index.values]

        info = 'Problem jurisdictions\n'
        for col_name, problems in prob_col.items():
            info += f'{col_name} ({len(problems)}):\n    '
            for county_info in problems:
                info += f"    {county_info}'\n    "
        print(info)
        
        self.assertTrue(len(prob_col), 0)


# --------------------------- Main ----------
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()