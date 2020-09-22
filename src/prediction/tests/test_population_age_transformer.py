'''
Created on Sep 21, 2020

@author: paepcke
'''
import unittest

import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), '.'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd

from voter_turnout_prediction import StatePredictor
from population_age_transformer import PopulationAgeTransformer

TEST_ALL = True
#TEST_ALL = False

class TestPopulationAgeTransformer(unittest.TestCase):

    DATA_DIR = os.path.join(os.path.dirname(__file__), '../../../data')

    #------------------------------------
    # setUpClass
    #-------------------

    @classmethod
    def setUpClass(cls):
        '''
        Read the state-to-state_abbreviation .csv, and 
        create
                            'State', 'Year'
           ('AK', 2008)    'Alabama'  2008
              ...             ...     2008
        
        @param cls:
        @type cls:
        '''
        super().setUpClass()
        state_abbrevs = pd.read_csv(os.path.join(cls.DATA_DIR, 'state_abbrevs.csv'),
                                    names=['State', 'Abbrev']
                                    )
        # Add ('US',2008) row
        #***state_abbrevs.loc[0] = pd.Series(['United States', 2008], index=('US', 2008))
        state_abbrevs.index = zip(state_abbrevs['Abbrev'], [2008]*len(state_abbrevs['Abbrev']))
        state_abbrevs = state_abbrevs.drop('Abbrev', axis=1)
        state_abbrevs.insert(1,'Year',[2008]*len(state_abbrevs['State']))
        us_row = pd.DataFrame({'State': ['United States'],
                               'Year' : [2008]
                               }, index=[('US', 2008)]
                               )

        cls.state_abbrevs = pd.concat([us_row, state_abbrevs])

    #------------------------------------
    # setUp 
    #-------------------

    def setUp(self):
        
        self.age2008_testfile = os.path.join(os.path.dirname(__file__),
                                                             'age2008.csv')

    #------------------------------------
    # tearDown 
    #-------------------

    def tearDown(self):
        pass

    #------------------------------------
    # test_intake 
    #-------------------

    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_intake(self):
        
        xformer = PopulationAgeTransformer({2008 : self.age2008_testfile})
        self.assertTrue(all(xformer.all_elections_df.columns.values == \
                        ['State','Year', 'Age_19_25','Age_26_34',
                         'Age_35_54','Age_55_64','Age_65_up']))
        self.assertEqual(len(xformer.all_elections_df), 52)
        
    #------------------------------------
    # test_transform 
    #-------------------
    
    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_transform(self):
        # Read ground truth df:
        grnd_truth = pd.read_pickle(os.path.join(os.path.dirname(__file__),
                                                 'transform_test_ground_truth.pickle'))
        xformer = PopulationAgeTransformer({2008 : self.age2008_testfile})
        df = xformer.transform(self.state_abbrevs)
        self.assertTrue(all(df == grnd_truth))

    #------------------------------------
    # test_fit_transform 
    #-------------------
    
    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_fit_transform(self):
        # Read ground truth df:
        grnd_truth = pd.read_pickle(os.path.join(os.path.dirname(__file__),
                                                 'transform_test_ground_truth.pickle'))
        xformer = PopulationAgeTransformer({2008 : self.age2008_testfile})
        df = xformer.fit_transform(self.state_abbrevs)
        self.assertTrue(all(df == grnd_truth))

# -------------- Utils -----------


# ------------------------ Main ------------

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()