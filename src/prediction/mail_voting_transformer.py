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

    def __init__(self, file_dict, county_level=False):
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
                df = self.handle_2018(file_dict[year],county_level)
            elif year == 2016:
                df = self.handle_2016(file_dict[year],county_level)
            elif year == 2014:
                df = self.handle_2014(file_dict[year],county_level)
            elif year == 2012:
                df = self.handle_2012(file_dict[year],county_level)

            if self.all_elections_df is None:
                self.all_elections_df = df
            else:
                self.all_elections_df = pd.concat([self.all_elections_df, df])

    #------------------------------------
    # handle_2018 
    #-------------------
    
    def handle_2018(self, file_name, county_level):
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
        
        NOTE: County level information was not yet available for
              the 2018 election. So we cannot use the method
              etl_election_survey() below, which is common to
              all other years.
        
        @param file_name: path to EAVS survey results
        @type file_name: str
        @return: information on use of vote by mail.
        @rtype: pd.DataFrame
        '''
        if county_level:
            raise NotImplementedError("County level not yet available for 2018")
        
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
        
        # Remove Guam and others Virgin Islands:
        df = df.drop(df[df['state'] == 'U.S. Virgin Islands'].index.item(),
                     axis=0)
        df = df.drop(df[df['state'] == 'Guam'].index.item(),
                     axis=0)
        df = df.drop(df[df['state'] == 'American Samoa'].index.item(),
                     axis=0)
        df = df.drop(df[df['state'] == 'Puerto Rico'].index.item(),
                     axis=0)

        # Rename the State entry 'U.S. Total' to 'United States'
        # to match how we have it elsewhere:
        
        #df.at[df[df['state'] == 'U.S. Total'].index.item(), 
        #      'state'] = 'United States'
        
        # Drop US total:
        df = df.drop(df[df['state'] == 'U.S. Total'].index.item(),
                     axis=0)

        # Turn the full State names into abbreviations:
        abbrevs = self.utils.state_abbrev_series(df.state)

        # Create a multiindex ['Region', 'Election'] with the
        # State and the year 2018 for joining with the main 
        # data table:
        indx = pd.MultiIndex.from_tuples(zip(abbrevs, [2018]*len(df)), 
                                         names=['Region','Election'])
        df.index = indx
        
        # No longer need State column, b/c we have
        # that info in the index:
        df = df.drop('state', axis=1)

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
        year = 2016
        
        self.log.info(f"Reading mail voting spreadsheet for {year}...")
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
        self.log.info(f"Done reading mail voting spreadsheet for {year}.")        
        
        # Now have:
        #      State   JurisdictionName     F1a                      F1g
        # 0       AK             ALASKA  323288  -888888: Not Applicable
        # 1       AL     AUTAUGA COUNTY   25146  -888888: Not Applicable
        # 2       AL     BALDWIN COUNTY   96229  -888888: Not Applicable
        # 3       AL     BARBOUR COUNTY   10544  -888888: Not Applicable
        # 4       AL        BIBB COUNTY    8853  -888888: Not Applicable
        # ...    ...                ...     ...                      ...
        # 6462    WY  SWEETWATER COUNTY   16430  -888888: Not Applicable
        # 6463    WY       TETON COUNTY   12769  -888888: Not Applicable
        # 6464    WY       UINTA COUNTY    8544  -888888: Not Applicable
        # 6465    WY    WASHAKIE COUNTY    3855  -888888: Not Applicable
        # 6466    WY      WESTON COUNTY    3578  -888888: Not Applicable
        # 
        # [6467 rows x 4 columns]
        
        # Rename cols:
        df = sheet.rename({'F1a' : f'VotesCast{year}',
                           'F1g' : f'ByMailReturned{year}',
                           'JurisdictionName' : 'Jurisdiction'
                           },axis=1)
        
        # This df is now in standard format for the County election survey
        # responses to stats in their jurisdiction. From here we
        # proceed the same for all election years:
        
        final_df = self.etl_election_survey(df, year, county_level)
        
        return final_df

    #------------------------------------
    # handle_2014
    #-------------------

    def handle_2014(self, file_name, county_level=False):
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
        
          'VotesCast2014', 'ByMailReturned', 'ByMailPerc'
        
        @param file_name: path to Excel file
        @type file_name: str
        @return information on votes counted, how many
            were mail-ins, and percentage.
        '''
        year = 2014
        
        self.log.info(f"Reading mail voting spreadsheet for {year}...")
        # State is a State abbreviation
        # F1a is total votes counted, and F1g is raw
        # number of how many voted by mail.

        sheet = pd.read_excel(io=file_name,
                              header=[0],
                              usecols=['State',
                                       'Jurisdiction',
                                       'QF1a',
                                       'QF1g']
                              )
        self.log.info(f"Done reading spreadsheet for {year}.")
        # Now have:
        #          State    QF1a                     QF1g
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
        df = sheet.rename({'QF1a' : f'VotesCast{year}',
                           'QF1g' : f'ByMailReturned{year}'
                           },axis=1)
        final_df = self.etl_election_survey(df, year, county_level)
        return final_df

    #------------------------------------
    # handle_2012
    #-------------------

    def handle_2012(self, file_name, county_level=False):
        '''
        See comments for handle_2016 for details.
        
        @param file_name: path to Excel file
        @type file_name: str
        @return information on votes counted, how many
            were mail-ins, and percentage.
        '''
        year = 2012
        
        self.log.info(f"Reading mail voting spreadsheet for {year}...")
        # State is a State abbreviation
        # F1a is total votes counted, and F1g is raw
        # number of how many voted by mail.

        sheet = pd.read_excel(io=file_name,
                              header=[0],
                              usecols=['State',
                                       'Jurisdiction',
                                       'QF1aBallotsCast',
                                       'QF1gVoteByMail']
                                       )
        self.log.info(f"Done reading spreadsheet for {year}.")
        # Now have:
        #      State       Jurisdiction  QF1aBallotsCast  QF1gVoteByMail
        # 0       AK             ALASKA         302465.0             NaN
        # 1       AL     AUTAUGA COUNTY          24065.0       -999999.0
        # 2       AL     BALDWIN COUNTY          85873.0       -999999.0
        # 3       AL     BARBOUR COUNTY          11534.0       -999999.0
        # 4       AL        BIBB COUNTY           8454.0       -999999.0
        # ...    ...                ...              ...             ...
        # 8149    WY  SWEETWATER COUNTY          17041.0             NaN
        # 8150    WY       TETON COUNTY          11507.0             NaN
        # 8151    WY       UINTA COUNTY           8578.0             NaN
        # 8152    WY    WASHAKIE COUNTY           3979.0             NaN
        # 8153    WY      WESTON COUNTY           3399.0             NaN
        # 
        # [8154 rows x 4 columns]

        # Rename cols:
        df = sheet.rename({'QF1aBallotsCast' : f'VotesCast{year}',
                           'QF1gVoteByMail' : f'ByMailReturned{year}'
                           },axis=1)
        final_df = self.etl_election_survey(df, year, county_level)
        return final_df


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

    #------------------------------------
    # etl_election_survey 
    #-------------------
    
    def etl_election_survey(self, df, year, county_level=False):
        '''
        Given an excerpt from election survey results 
        reported by Counties for one year, clean that
        excerpt, and return it in a standard format for
        the transform() method to handle. Input is expected
        to look something like this:
        
             State       Jurisdiction VotesCast2016       ByMailReturned2016
        0       AK             ALASKA        323288  -888888: Not Applicable
        1       AL     AUTAUGA COUNTY         25146  -888888: Not Applicable
        2       AL     BALDWIN COUNTY         96229  -888888: Not Applicable
        3       AL     BARBOUR COUNTY         10544  -888888: Not Applicable
        4       AL        BIBB COUNTY          8853  -888888: Not Applicable
        ...    ...                ...           ...                      ...
        6462    WY  SWEETWATER COUNTY         16430  -888888: Not Applicable
        6463    WY       TETON COUNTY         12769  -888888: Not Applicable
        6464    WY       UINTA COUNTY          8544  -888888: Not Applicable
        6465    WY    WASHAKIE COUNTY          3855  -888888: Not Applicable
        6466    WY      WESTON COUNTY          3578  -888888: Not Applicable
        
        [6467 rows x 4 columns]
        
        All other columns are assumed to have been removed.

        @param df: standard exerpts from one year's 
             election stats surveys 
        @type df: pd.DataFrame
        @param year: the election year
        @type year: int
        @param county_level: whether or not survey info
            should be retained to the county level, or
            only to the State level
        @type county_level: bool
        @return: a df that looks the same for all
            the years.
        @rtype: pd.DataFrame
        '''

        # Remove Guam and U.S. Virgin Islands (if they
        # are included; no harm if they are absent:
        df = df.drop(df[df['State'].isin(['GU','VI', 'PR'])].index)
        
        # Throw away info on no mail-in for now by
        # setting the Not Applicable and Not Available
        # to zero:
        df = df.replace(to_replace='-888888: Not Applicable', value=0)
        df = df.replace(to_replace='-999999: Data Not Available', value=0)
        # Sometimes the text is omitted:
        df = df.replace(to_replace=-888888, value=0)
        df = df.replace(to_replace=-999999, value=0)

        # Ensure no nan:
        df = df.fillna(0, axis=0)
        
        # Ensure that ByMailReturned are all ints, not strings:
        df = df.astype(dtype={f'VotesCast{year}' : int, 
                              f'ByMailReturned{year}' : int})

        # Create a multiindex ['Region', 'Election'] with the
        # State and the year for joining with the main 
        # data table:
        
        if county_level:
            # Need a 3-tier multiindex: Region/County/Election:
            region   = df.State
            county   = df.Jurisdiction
            election = [year]*len(region)*len(county)
            indx = pd.MultiIndex.from_tuples(zip(region,county,election), 
                                             names=['Region','County', 'Election'])
            df.index = indx
            # Remove cols State and Jurisdiction, b/c
            # they are now in the index:
            df = df.drop(['State', 'Jurisdiction'], axis=1)
        else:
            # Need 2-tier multiindex: Region/Election:
            indx = pd.MultiIndex.from_tuples(zip(df.State, [year]*len(df)), 
                                             names=['Region','Election'])
            df.index = indx
            # Aggregate county data for each State:
            grp = df.groupby(level=['Region', 'Election'])
            # The following loses the State and Jurisdiction
            # cols, b/c they are non-numeric. That's fine, since
            # State is in the index now:
            df  = grp.sum()

        # Add the 'ByMailPerc' col:
        df[f'ByMailPerc{year}'] = df[f'ByMailReturned{year}'] / df[f'VotesCast{year}']

        # Now have something like this if State level:
        #                  VotesCast2016  ByMailReturned2016  ByMailPerc2016
        # Region Election                                                   
        # AK     2016             323288                   0        0.000000
        # AL     2016            2137452                   0        0.000000
        #             ...
        return df




