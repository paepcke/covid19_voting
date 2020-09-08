'''
Created on Sep 4, 2020

@author: paepcke
'''

import argparse
import os
import sys
import csv

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier


class StatePredictor(object):
    '''
    classdocs
    '''
    VOTER_TURNOUT_FILES = {2008: os.path.join(os.path.dirname(__file__),
                                       '../../data/turnoutRates2008NovemberGeneralElection.xlsx'),
                           2012: os.path.join(os.path.dirname(__file__),
                                            '../../data/turnoutRates2012NovemberGeneralElection.xlsx'),
                           2016: os.path.join(os.path.dirname(__file__),
                                            '../../data/turnoutRates2016NovemberGeneralElection.xlsx')
                           }

    #------------------------------------
    # Constructor 
    #-------------------

    def __init__(self, csv_file, label_col='State'):
        '''
        Constructor
        '''
        
        # Directory with various data files:
        self.data_dir = os.path.join(os.path.dirname(__file__), '../../data')
        
        # Initialize various mappings (State names to their abbrevs, etc.),
        self.import_state_mappings()
        
        # Import voter turnout:
        voter_turnout = self.import_voter_turnout(self.VOTER_TURNOUT_FILES)

        # Import csv file with Google query statistics:
        search_features = self.import_search_data(csv_file)
        
        # Join voter turnout and search frequencies 
        # into one wide table:
        features = voter_turnout.merge(search_features, 
                                       left_on='State Abv', 
                                       right_on='State', 
                                       how='inner')

        # Get values in column that we are to predict:
        y = search_features[label_col]
        # Remove that col from features:
        X = search_features.drop(columns=label_col)

        rand_forest = RandomForestClassifier()
        rand_forest.fit(X,y)
        print(rand_forest)

    #------------------------------------
    # import_search_data 
    #-------------------
    
    def import_search_data(self, csv_file):
        '''
        Read CSV file, and return a tuple,
        (x,Y). Value x is a dataframe containing
        the feature vectors. Value Y is a sequence
        that are the predicted values.
        
        Assumptions:
           o First seven columns are query term counts for each
             day of the week.
           o Last column is label to predict (State in this
             case, but could be voter turnout)
             
             Example rows:
                 76,100,92,54,71,69,46,0     < week3 before election State 0
                 76,57,67,85,100,96,99,0     < week2 before election State 0
                 43,60,52,100,73,80,47,0     < week1 before election State 0
                 100,67,0,61,42,33,84,1      < week3 before election State 1
                 0,47,100,55,56,46,50,1      < week2 before election State 1
                 44,42,65,49,100,42,81,1     < week1 before election State 1
                 61,42,77,100,79,44,91,2                   ...       State 2

           o Consecutive rows are for weeks before election.
             Example: if data contains two weeks for each 
             label (State/voter turnout), then these rows
             represent data for two consecutive weeks before
             the election.
             
        Returns a dataframe, where '*' is the sum of searches 
        over all States on that day:
                State week  Mon   Tue  Wed  Thu  Fri  Sat  Sun 
           ...    AL    0   76  100   92   54   71   69   46
                  AL    1   76   57   67   85  100   96   99
                  AL    2   43   60   52  100   73   80   47
                  AK    0  100   67    0   61   42   33   84
                    ...
                  US    0    *   *    *    *    *    *    * 
                  US    1    *   *    *    *    *    *    * 
                  US    2    *   *    *    *    *    *    *

        @param csv_file: comma separated data file 
        @type csv_file: str
        @return: tuple with matrix n x 8: one week indicator
            and seven day's of Google query search counts.
            Final column is state abbrev
        @rtype: (pd.DataFrame, pd.Series)
        '''
        df = pd.read_csv(csv_file,
                         names=['Mon','Tue','Wed','Thu','Fri','Sat','Sun','State'],
                         index_col=False, # 1st col is *not* an index col
                         )
        
        # Add a column that indexes the week before
        # the election that one row represents.
        # A few steps needed:
        
        # How many weeks of data for each State?
        # Take State 0 (any will do), and could how
        # often it occurs in the State column:
        
        num_weeks  = len(df[df.State == 0])

        # How many States are in the data? Find
        # by counting unique values in the States
        # column: 
        num_states = len(df.groupby('State').nunique())
        
        # Create as many [0,1,2,...] as there
        # are States. Ex:
        #   0, ... Alabama 
        #   1, ... Alabama 
        #   2, ... Alabama                 
        #   0, ... California
        #   1, ... California 
        #   2, ... California
        # ...
        
        week_indices = list(range(num_weeks)) * num_states 
        
        # Finally: add '0/1/2/0,1,2/...' as first col

        df.insert(0,'week',week_indices)
        
        # Replace the State codes (0,1,2...) with
        # the State abbreviations:
        
        df = df.replace(to_replace={'State': self.state_codings})

        # Build a row for each week that is
        # the some of all queries on one day
        # across all States:
        #
        #    week   Mon                     Tue
        #      0  sum(of Mondays of wk0)    sum(of Tuesdays of wk0)    ...
        #      1  sum(of Mondays of wk1)    sum(of Tuesdays of wk0)    ...
        
        df_final = df.copy()
        for wk in range(num_weeks):
            that_wk_only = df[df['week'] == wk]
            
            # Now sum all the cols (
            wk_summed = that_wk_only.sum(axis=0, numeric_only=True)

            # Now have something like:
            #     df.sum(axis=0, numeric_only=True)
            #     week      153
            #     Mon     10595
            #     Tue     10208
            #     Wed     10529
            #     Thu     11095
            #     Fri     10942
            #     Sat     10368
            #     Sun     10673
            #     dtype: int64
            #
            # So the week was summed as well, which is silly.
            
            wk_summed['week'] = wk

            # Add value 'US' for the 'State' column
            wk_summed['State'] = 'US'
            df_final = df_final.append(wk_summed, ignore_index=True)

        # Finally, move the State column into left-most position:
        df_final = df_final.reindex(columns=['State', 'week', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
        return df_final
    
    #------------------------------------
    # import_state_mappings
    #-------------------
    
    def import_state_mappings(self):
        '''
        Initializes States related lookup dicts:
        
        1. self.state_codings: mapping the integer codes for States
           to the 2-letter State names:
        
           ID    State
           {0   :   AL
           1   :   AK
              ...}

        2. self.state_abbrevs: long State names to 2-letter abbreviations
        
           LongName  :   Abbrev
          {'New York' :   'NY'
               ...}
        
        @return: dataframe with mapping
        @rtype: pd.DataFrame 
        '''
        
        # Integer state codes to two-letter abbreviation:
        self.state_codings = {}
        
        # The 'encodings...' is needed because the CSV
        # file seems to include a leading '\ufeff' to 
        # indicate Endianness. Specifying encoding 
        # has Python remove that indicator:
        
        with open(os.path.join(self.data_dir, 'states.csv'), 'r', encoding='utf-8-sig') as fd:
            reader = csv.reader(fd)
            for (coding, state_abbrev) in reader:
                self.state_codings[int(coding)] = state_abbrev

        # Long State name to 2-letter abbreviation:
        #
        #    LongName    Abbrev
        #   "New York"    "NY"
        #           ...

        self.state_abbrevs = {}
        with open(os.path.join(self.data_dir, 'state_abbrevs.csv'), 'r', encoding='utf-8-sig') as fd:
            reader = csv.reader(fd)
            for (longName, abbrev) in reader:
                self.state_abbrevs[longName] = abbrev

    #------------------------------------
    # import_voter_turnout 
    #-------------------
    
    def import_voter_turnout(self, excel_file_dict):
        '''
        Import an Excel sheet of voter turnouts
        as provided for download at
        http://www.electproject.org/home/precinct_data 
        
        The Excel sheet is read 
            > df.columns 
            Index(['index', 'State', 'Website', 'Status', 'VEP Total Ballots Counted',
                   'VEP Highest Office', 'VAP Highest Office', 'Total Ballots Counted',
                   'Highest Office', 'Voting Eligible Population', 'Voting Age Population',
                   'Non Citizen Perc', 'Prison', 'Probation', 'Parole',
                   'Total Ineligible Felons', 'Overseas Eligible', 'State Abbrv'],
                  dtype='object')
                  
        @param excel_src: path to spreadshee of one elections as
            provided by the Election Project.
        @type excel_src: src
        @return: data frame with the above columns.
        @rtype: pd.DataFrame

        '''
        
        voter_turnout_df = None
        for year in excel_file_dict.keys():
            
            excel_src = excel_file_dict[year]

            # The 'header=[0,1]' notifies the read method
            # that the first two rows are taken by a nested
            # header. The result will feature a multiindex:
            df = pd.read_excel(excel_src, header=[0,1])

            # Here is magic to deal with the complex,
            # merged header columns. The solution comes from: 
            # https://stackoverflow.com/questions/42132663/fix-dataframe-columns-when-reading-an-excel-file-with-a-header-with-merged-cells
    
            df = df.reset_index()

            # Turn multiindex level names that Pandas brought in
            # as 'Unnamed: ...' to emtpy strings:
            df = df.rename(columns=lambda x: x if not 'Unnamed' in str(x) else '')

            # Drop multi-index level 'Turnout Rates', 'Numerators, 
            # 'Denominators', 'VEP Components (Modifications to VAP to Calculate VEP) 
            # We only want the flat col header of level 1:
            df.columns = df.columns.droplevel(0)
            
            # File 2016 has two extra columns: 'State Results Website',
            # and 'Status'. Remove those:
            if year == 2016:
                # Remove the columns:
                df = df.drop(columns=['State Results Website', 'Status'])
            
            # File 2008 is missing the, 'State Abv' column. Add that:
            if year == 2008:
                # Add the State Abv col, leaving the first row 
                # NaN for now (that's US total):
                state_abbrevs = self.state_abbrev_series(df.iloc[:,1])
                df.insert(len(df.columns), 'State Abv', state_abbrevs)
                
            # For all years, add 'US' as the 'State abbreviation':
            df.iloc[0,-1] = 'US' 
            
            # 16 columns (each of which corresponds to a level in
            # the multiindex:
            df.columns = ['index_dup', 'State', 'VEP Total Ballots Counted', 'VEP Highest Office',
                   'VAP Highest Office', 'Total Ballots Counted', 'Highest Office', 'Voting Eligible Population',
                   'Voting Age Population',
                   'Non Citizen Perc',
                   'Prison',
                   'Probation',
                   'Parole',
                   'Total Ineligible Felons',
                   'Overseas Eligible',
                   'State Abv']
    
            # Add a year col just after the State:
            df.insert(2,'Year',[year]*len(df)) 
            
            # Remove the index_dup col:
            df = df.drop(columns='index_dup')

            # We use 'VEP Total Ballots Counted' for voter participation. 
            # But some States don't report this number. Their value will 
            # be NaN. In that case we use the 'VEP Highest Office' percentage:
            
            df['VEP Total Ballots Counted'] = df['VEP Total Ballots Counted'].fillna(df['VEP Highest Office'])
    
            # The last row may have 'Notes' in it, delete such a row:
            #if df['State']['Unnamed: 0_level_1'].iloc[-1][-1].startswith('Note:'):
            #    us_plus_state_names = us_plus_state_names[:-1]['United States']
             
            # The State column may have row(s) that are notes.
            # We hope they'll continue to start with 'Note':
            df = df.drop(df[df.State.str.startswith('Note')].index)
            
            if voter_turnout_df is None:
                voter_turnout_df = df.copy()
            else:
                voter_turnout_df = voter_turnout_df.append(df.copy())
        
        return voter_turnout_df
        
# ------------------------ Utilities ----------

    def state_abbrev_series(self, state_name_series):
        # One 'state' name in the voter turnout Excel
        # sheets is 'United States', leave that abbrev
        # NaN to conform with the newer turnout sheets:
        
        res = state_name_series.apply(lambda state_nm: np.nan \
                                      if state_nm == 'United States' else self.state_abbrevs[state_nm])
        return res

# ------------------------ Main ------------
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     description="Predict State from Google queries"
                                     )

    parser.add_argument('-l', '--errLogFile',
                        help='fully qualified log file name to which info and error messages \n' +\
                             'are directed. Default: stdout.',
                        dest='errLogFile',
                        default=None);
    parser.add_argument('csv_file',
                        help='Path to data file'
                        )

    args = parser.parse_args();

    if not os.path.exists(args.csv_file):
        print(f"Cannot find data file '{args.csv_file}'")

    StatePredictor(args.csv_file)
