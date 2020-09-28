'''
Created on Sep 27, 2020

@author: paepcke
'''

from sklearn.base import BaseEstimator, TransformerMixin

import pandas as pd
from prediction.covid_utils import CovidUtils
from utils.logging_service import LoggingService


class MailVotingTransformer(BaseEstimator, TransformerMixin):

    #------------------------------------
    # Constructor 
    #-------------------

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
        self.log = LoggingService()

        for year in file_dict.keys():

            if year == 2018:
                self.handle_2018(file_dict[year])
            if year == 2016:
                self.handle_2016(file_dict[year])

    #------------------------------------
    # handle_2018 
    #-------------------
    
    def handle_2018(self, file_name):
        '''
        Takes path to Excel sheet with mail-in info
        from the Election Administration and Voting Survey
        Returns a dataframe:
        
            'VotesCast2018', 'ByMailReturned', 'ByMailPerc'
        
        The votes_cast column is the raw number of votes cast
        in that row's State. The by_mail_returned column is 
        the raw number of votes cast that were sent by mail.
         
        The df will have a multiindex ['Region', 'Election']
        where Region is a State abbreviation, and Eleciton
        is a year.
        
        @param file_name: path to EAVS survey results
        @type file_name: str
        @return: information on use of vote by mail.
        @rtype: pd.DataFrame
        '''
        sheet = pd.read_excel(io=file_name,
                              header=[0,1,2])
        state = sheet['State']['Unnamed: 0_level_1']['Unnamed: 0_level_2']
        by_mail_returned = sheet['By-Mail Ballots Returned By Voters']['Total']['Unnamed: 3_level_2']
        votes_cast       = sheet['Total Voter Turnout']['Unnamed: 1_level_1']['Unnamed: 1_level_2']
        
        df = pd.DataFrame({'state': state,
              'VotesCast2018' : votes_cast,
              'ByMailReturned': by_mail_returned,
              'ByMailPerc' : by_mail_returned / votes_cast
              })
        
        # Cleanup:
        
        # the Oregon entry in the 'state' column is like this:
        #    'Oregon [1,2]'. Turn that into 'Oregon':
        df.at[df[df['state'].str.startswith('Oregon')].index.item(), 'state'] = 'Oregon'

        # Several other state entries have a single 
        # footnote instead: 'North Dakota [2]' and 
        # 'New York [1]'. Remove the footnotes:
        problem_indexes = df[df['state'].str.endswith(']')].index
        for indx in problem_indexes:
            df.at[indx,'state'] = df.at[indx,'state'][:-4]
        
        # Remove Guam and Virgin Islands:
        df = df.drop(df[df['state'] == 'U.S. Virgin Islands'].index.item(),
                     axis=0)
        df = df.drop(df[df['state'] == 'Guam'].index.item(),
                     axis=0)
        df = df.drop(df[df['state'] == 'American Samoa'].index.item(),
                     axis=0)

        # Rename the State entry 'U.S. Total' to 'United States'
        # to match how we have it elsewhere:
        
        #df.at[df[df['state'] == 'U.S. Total'].index.item(), 
        #      'state'] = 'United States'
        
        # Drop US total:
        df = df.drop(df[df['state'] == 'U.S. Total'].index.item(),
                     axis=0)

        # Create a multiindex ['Region', 'Election'] with the
        # State and the year 2018 for joining with the main 
        # data table:
        indx = pd.MultiIndex.from_tuples(zip(df.State, [2018]*len(df)), 
                                         names=['Region','Election'])
        df.index = indx
        
        # No longer need State column, b/c we have
        # that info in the index:
        df = df.drop('State', axis=1)

        return df
    
    #------------------------------------
    # handle_2016 
    #-------------------

    def handle_2016(self, file_name, county_level=False):
        '''
        This spreadsheet contains information about
        each 'Jurisdiction', which are mostly counties.
        Therefore: many rows per State.
        
        The relevant sheet is 'Section F', which holds
        ballot counts, and vote-by-mail counts. The
        'voted by mail' col may contain:
        
            -888888: Not Applicable
            -999999: Data Not Available
            
        So we can deduce which States had no vote-by-mail.

        Return a dataframe with multiindex ['Region', 'Election']
        where Region is a State abbreviation, and Election is
        a year. Columns will be:
        
          'VotesCast2016', 'ByMailReturned', 'ByMailPerc'
        
        @param file_name: path to Excel file
        @type file_name: str
        @return information on votes counted, how many
            were mail-ins, and percentage.
        '''
        self.log.info("Reading mail voting spreadsheet for 2016...")
        # State is a State abbreviation
        # F1a is total votes counted, and F1g is raw
        # number of how many voted by mail.

        sheet = pd.read_excel(io=file_name,
                              header=[0],
                              sheet_name='SECTION F',
                              usecols=['State',
                                       'JurisdictionName',
                                       'F1a',
                                       'F1g']
                              )
        self.log.info("Done reading spreadsheet for 2016.")
        
        # Now have:
        #          State     F1a                      F1g
        #     0       AK  323288  -888888: Not Applicable
        #     1       AL   25146  -888888: Not Applicable
        #     2       AL   96229  -888888: Not Applicable
        #     3       AL   10544  -888888: Not Applicable
        #     4       AL    8853  -888888: Not Applicable
        #     ...    ...     ...                      ...
        #     6462    WY   16430  -888888: Not Applicable
        #     6463    WY   12769  -888888: Not Applicable
        #     6464    WY    8544  -888888: Not Applicable
        #     6465    WY    3855  -888888: Not Applicable
        #     6466    WY    3578  -888888: Not Applicable    
        
        # Rename cols:
        df = sheet.rename({'F1a' : 'VotesCast2016',
                           'F1g' : 'ByMailReturned'
                           },axis=1)
        
        # Throw away info on no mail-in for now by
        # setting the Not Applicable and Not Available
        # to zero:
        df = df.replace(to_replace='-888888: Not Applicable', value=0)
        df = df.replace(to_replace='-999999: Data Not Available', value=0)
        
        # Ensure no nan:
        df['ByMailReturned'] = df['ByMailReturned'].fillna(0, axis=0)
        
        # Ensure that ByMailReturned are all ints, not strings:
        df = df.astype(dtype={'VotesCast2016' : int, 'ByMailReturned' : int})

        # Create a multiindex ['Region', 'Election'] with the
        # State and the year 2016 for joining with the main 
        # data table:
        indx = pd.MultiIndex.from_tuples(zip(df.State, [2016]*len(df)), 
                                         names=['Region','Election'])

        df.index = indx
        # No longer need State as col since it's in the index:
        df = df.drop(labels=['State','JurisdictionName'], axis=1)
        
        if not county_level:
            # Aggregate county data for each State:
            grp = df.groupby(level='Region')
            df  = grp.sum()
            
        # Add the 'ByMailPerc' col:
        df['ByMailPerc'] = df['ByMailReturned'] / df['VotesCast2016']
        return df

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
        Adjoin the vote-by-mail information for each
        state to the passed-in df. 
        Assumptions: 
           o len(X) is number of States plus D.C, but w/o Puerto Rico
           o X.index are tuples (<State_abbrev>, <year>)
           
        Returns a copy of X with vote-by-mail columns
        attached to the right.
        new columns:
            VotesCast
            ByMailReturned
            ByMailPerc
           
        @param X: feature matrix 
        @type X: pd.DataFrame
        @return: copy of X with new columns appended
        @rtype: pd.DataFrame
        '''
        
        new_X = X.join(self.all_elections_df)
        return new_X
