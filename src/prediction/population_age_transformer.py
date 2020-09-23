'''
Created on Sep 21, 2020

@author: paepcke
'''

from sklearn.base import BaseEstimator, TransformerMixin

import pandas as pd
from prediction.covid_utils import CovidUtils


class PopulationAgeTransformer(BaseEstimator, TransformerMixin):
    '''
    Reads statistics on population age by State.
    Data is from 
    https://www.kff.org/other/state-indicator/distribution-by-age/?currentTimeframe=10&selectedRows=%7B%22states%22:%7B%22all%22:%7B%7D%7D,%22wrapups%22:%7B%22united-states%22:%7B%7D%7D%7D&sortModel=%7B%22colId%22:%22Location%22,%22sort%22:%22asc%22%7D
    '''

    def __init__(self, file_dict):
        '''
        Given a dictionary with keys (<State>,<year>),
        and values being full paths to CSV files that
        hold the data. One file for each year:
         
        @param file_dict: pointers to data files
        @type file_dict: {int : str}
        '''
        self.utils = CovidUtils()
        self.all_elections_df = None
        
        for year in file_dict.keys():
                
            df = pd.read_csv(file_dict[year],
                             skiprows=2,
                             usecols=lambda col_name: col_name not in ['Children 0-18',
                                                                       'Total',
                                                                       'Footnotes']
                             )

            # Make the column headers legal Python identifiers,
            # and change 'Location' to 'State':
            df = df.rename({'Location' : 'State',
                            'Adults 19-25' : 'Age_19_25',
                            'Adults 26-34' : 'Age_26_34',
                            'Adults 35-54' : 'Age_35_54',
                            'Adults 55-64' : 'Age_55_64',
                            '65+'          : 'Age_65_up'
            }, axis=1)

            # Remove all rows below Wyoming; they are notes
            wyoming_idx = df.loc[df['State'] == 'Wyoming'].index[0]
            df = df[df.index <= wyoming_idx]

            # Create a column of State abbreviations,
            # and make a multiindex (<state_abbrev>, <year>):
            abbrevs = self.utils.state_abbrev_series(df.State)
            
            indx = pd.MultiIndex.from_tuples(zip(abbrevs, 
                                                 [year]*len(abbrevs)
                                                 ), 
                                                 names=['Region','Election'])
            df.index = indx
            # No longer need State column, b/c we have
            # that info in the index:
            df = df.drop('State', axis=1)

            if self.all_elections_df is None:
                self.all_elections_df = df
            else:
                self.all_elections_df = pd.concat([self.all_elections_df, df])

    #------------------------------------
    # fit
    #-------------------

    def fit(self, X):
        return self
    
    #------------------------------------
    # transform 
    #-------------------

    def transform(self, X):
        '''
        Adjoin the age distribution for each
        state to the passed-in df. 
        Assumptions: 
           o len(X) is number of States plus D.C, but w/o Puerto Rico
           o X.index are tuples (<State_abbrev>, <year>)
           
        Returns a copy of X with age distribution in the following
        new columns:
            Age_19_25
            Age_26_34
            Age_35_54
            Age_55_64
            Age_65_up
           
        @param X: feature matrix 
        @type X: pd.DataFrame
        @return: copy of X with new columns appended
        @rtype: pd.DataFrame
        '''
        
        new_X = X.join(self.all_elections_df)
        return new_X