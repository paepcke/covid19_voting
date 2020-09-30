'''
Created on Sep 27, 2020

@author: paepcke
'''
import os
import unittest

import pandas as pd
from mail_voting_transformer import MailVotingTransformer

pd.set_option('display.max_columns', None)  
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', None)


#******TEST_ALL = True
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
        year = 2018
        xformer = MailVotingTransformer({year : os.path.join(self.DATA_DIR,
                                                             f'voteByMail{year}.xlsx')
                                         })
        df = xformer.all_elections_df
        self.assertEqual(df.index[0], ('AL', year))
        maine_data = pd.Series([646083,185763,0.287522])
        self.assertTrue(all(df.loc['ME',year].values.round(4) == maine_data.round(4)))
        # All sums should be positive, lest we missed 
        # a -888888 or -999999 code (Data not Applicable/Available)
        self.assertTrue(df[f'VotesCast{year}'].sum() > 0)
        self.assertTrue(df[f'VotesCast{year}'].sum() > 0)

    #------------------------------------
    # test_2016_intake_state_level 
    #-------------------

    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_2016_intake_state_level(self):
        year = 2016
        xformer = MailVotingTransformer({year : os.path.join(self.DATA_DIR,
                                                             f'voteByMail{year}.xlsx')
                                         })
        # xformer.all_elections_df should now start with:
        # 
        #                  VotesCast2016  ByMailReturned2016  ByMailPerc2016
        # Region Election                                                   
        # AK     2016             323288                   0        0.000000
        # AL     2016            2137452                   0        0.000000
        # AR     2016            1048513                 867        0.000827
        # AZ     2016            2722660                   0        0.000000
        # CA     2016           14610494               21734        0.001488
        # CO     2016            2884199             2654993        0.920530
        #      ...

        df = xformer.all_elections_df
        expected = pd.Series({f'VotesCast{year}': 1.048513e+06,
                              f'ByMailReturned{year}': 8.670000e+02,
                              f'ByMailPerc{year}': 8.268853e-04
                              }, name=('AR',year)
                              )        
        self.assertTrue(all(df.loc[('AR', year)]), expected)
        self.assertEqual(df.index[0], ('AK', year))
        # One row each for States + D.C. + 'US':
        self.assertEqual(len(df.index), 52)
        # All sums should be positive, lest we missed 
        # a -888888 or -999999 code (Data not Applicable/Available)
        self.assertTrue(df[f'VotesCast{year}'].sum() > 0)
        self.assertTrue(df[f'VotesCast{year}'].sum() > 0)


    #------------------------------------
    # test_2016_intake_county_level 
    #-------------------
    
    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_2016_intake_county_level(self):
        year=2016
        xformer = MailVotingTransformer({year : os.path.join(self.DATA_DIR,
                                                             f'voteByMail{year}.xlsx')
                                         }, county_level=True)
        
        # Result in xformer.all_elections_df is like this:
        
        #                                    VotesCast2016  ByMailReturned2016  ByMailPerc2016
        # Region County            Election                                                   
        # AK     ALASKA            2016             323288                   0             0.0
        # AL     AUTAUGA COUNTY    2016              25146                   0             0.0
        #        BALDWIN COUNTY    2016              96229                   0             0.0
        #        BARBOUR COUNTY    2016              10544                   0             0.0
        #        BIBB COUNTY       2016               8853                   0             0.0
        # ...                                          ...                 ...             ...
        # WY     SWEETWATER COUNTY 2016              16430                   0             0.0
        #        TETON COUNTY      2016              12769                   0             0.0
        #        UINTA COUNTY      2016               8544                   0             0.0
        #        WASHAKIE COUNTY   2016               3855                   0             0.0
        #        WESTON COUNTY     2016               3578                   0             0.0
        # 
        # [6465 rows x 3 columns]


        df = xformer.all_elections_df
        # Get the first three counties
        self.assertTrue(all(df.index.get_level_values('County')[:3] == 
            ['ALASKA', 'AUTAUGA COUNTY', 'BALDWIN COUNTY']))
        self.assertEqual(df.loc[('AK','ALASKA',2016)][f'VotesCast{year}'], 323288)
        # All sums should be positive, lest we missed 
        # a -888888 or -999999 code (Data not Applicable/Available)
        self.assertTrue(df[f'VotesCast{year}'].sum() > 0)
        self.assertTrue(df[f'VotesCast{year}'].sum() > 0)


    #------------------------------------
    # test_2012_intake_county_level 
    #-------------------
    
    @unittest.skipIf(not TEST_ALL, 'skipping temporarily')
    def test_2012_intake_county_level(self):
        year = 2012
        xformer = MailVotingTransformer({year : os.path.join(self.DATA_DIR,
                                                             f'voteByMail{year}.xlsx')
                                         }, county_level=True)
        
        # Result in xformer.all_elections_df is like this:
        #                                    VotesCast2012  ByMailReturned2012  ByMailPerc2012
        # Region County            Election                                                   
        # AK     ALASKA            2012              10596                9628        0.908645
        # AL     AUTAUGA COUNTY    2012                 76                   0        0.000000
        #        BALDWIN COUNTY    2012                172                   0        0.000000
        #        BARBOUR COUNTY    2012                 23                   0        0.000000
        #        BIBB COUNTY       2012                  0                   0             NaN
        # ...                                          ...                 ...             ...
        # WY     SWEETWATER COUNTY 2012                 63                  56        0.888889
        #        TETON COUNTY      2012                 88                  86        0.977273
        #        UINTA COUNTY      2012                 38                  38        1.000000
        #        WASHAKIE COUNTY   2012                 21                  21        1.000000
        #        WESTON COUNTY     2012                 28                  15        0.535714
        # 
        # [8153 rows x 3 columns]

        df = xformer.all_elections_df
        # Get the first three counties
        self.assertTrue(all(df.index.get_level_values('County')[:3] == 
            ['ALASKA', 'AUTAUGA COUNTY', 'BALDWIN COUNTY']))
        self.assertEqual(df.loc[('AK','ALASKA',year)][f'VotesCast{year}'], 10596)
        # All sums should be positive, lest we missed 
        # a -888888 or -999999 code (Data not Applicable/Available)
        self.assertTrue(df[f'VotesCast{year}'].sum() > 0)
        self.assertTrue(df[f'VotesCast{year}'].sum() > 0)

    #------------------------------------
    # test_2012_intake_state_level 
    #-------------------

    #****@unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_2012_intake_state_level(self):
        year = 2012
        xformer = MailVotingTransformer({year : os.path.join(self.DATA_DIR,
                                                             f'voteByMail{year}.xlsx')
                                         })
        # xformer.all_elections_df should now start with:
        # 
        #                     VotesCast2012  ByMailReturned2012  ByMailPerc2012
        #
        # Region Election                                                   
        # AK     2012              10596                9628        0.908645
        # AL     2012           -4996409                1538       -0.000308
        # AR     2012           -2997859                1692       -0.000564
        # AS     2012                116                 116        1.000000
        # AZ     2012               9445                8949        0.947485
        # CA     2012              63193               35595        0.563274
        # CO     2012              17363               15777        0.908656

        df = xformer.all_elections_df
        expected = pd.Series({f'VotesCast{year}': 1080809,
                              f'ByMailReturned{year}': 1904,
                              f'ByMailPerc{year}': 0.001762
                              }, name=('AR',year)
                              )        
        self.assertTrue(all(df.loc[('AR', year)]), expected)
        self.assertEqual(df.index[0], ('AK', year))
        # One row each for States + D.C. + 'US':
        self.assertEqual(len(df.index), 52)
        # All sums should be positive, lest we missed 
        # a -888888 or -999999 code (Data not Applicable/Available)
        self.assertTrue(df[f'VotesCast{year}'].sum() > 0)
        self.assertTrue(df[f'VotesCast{year}'].sum() > 0)


# --------------- Main ----------
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testIntake']
    unittest.main()