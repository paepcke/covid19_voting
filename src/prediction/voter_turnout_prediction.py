'''
Created on Sep 4, 2020

@author: paepcke
'''

import csv
import os

from category_encoders.leave_one_out import LeaveOneOutEncoder
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import mean_squared_error
from sklearn.metrics import make_scorer

import matplotlib.pyplot as plt
import seaborn as sns

import pandas as pd
import numpy as np

from utils.logging_service import LoggingService

class StatePredictor(object):
    '''
    classdocs
    '''
    VOTER_TURNOUT_FILES = {
                           2004: os.path.join(os.path.dirname(__file__),
                                       '../../data/turnoutRates2004NovemberGeneralElection.xlsx'),
                           2006: os.path.join(os.path.dirname(__file__),
                                       '../../data/turnoutRates2006NovemberGeneralElection.xlsx'),
                           2008: os.path.join(os.path.dirname(__file__),
                                       '../../data/turnoutRates2008NovemberGeneralElection.xlsx'),
                           2010: os.path.join(os.path.dirname(__file__),
                                       '../../data/turnoutRates2010NovemberGeneralElection.xlsx'),
                           2012: os.path.join(os.path.dirname(__file__),
                                            '../../data/turnoutRates2012NovemberGeneralElection.xlsx'),
                           2014: os.path.join(os.path.dirname(__file__),
                                       '../../data/turnoutRates2014NovemberGeneralElection.xlsx'),
                           2016: os.path.join(os.path.dirname(__file__),
                                            '../../data/turnoutRates2016NovemberGeneralElection.xlsx'),
                           2018: os.path.join(os.path.dirname(__file__),
                                       '../../data/turnoutRates2018NovemberGeneralElection.xlsx'),
                           }
    
    QUERY_TERM_FILES    = {
                           2004: os.path.join(os.path.dirname(__file__),
                                            '../../data/dataset_2004_vote.csv'),      # 'vote'
                           2006: os.path.join(os.path.dirname(__file__),
                                            '../../data/dataset_2006_vote.csv'),
                           2008: os.path.join(os.path.dirname(__file__),
                                            '../../data/dataset_2008_vote.csv'),
                           2010: os.path.join(os.path.dirname(__file__),
                                            '../../data/dataset_2010_vote.csv'),
                           2012: os.path.join(os.path.dirname(__file__),
                                            '../../data/dataset_2012_vote.csv'),
                           2014: os.path.join(os.path.dirname(__file__),
                                            '../../data/dataset_2014_vote.csv'),
                           2016: os.path.join(os.path.dirname(__file__),
                                            '../../data/dataset_2016_vote.csv'),
                           20018: os.path.join(os.path.dirname(__file__),
                                            '../../data/dataset_2018_vote.csv'),
                           2004: os.path.join(os.path.dirname(__file__),
                                            '../../data/dataset_2004_elections.csv'),  # 'elections'
                           2006: os.path.join(os.path.dirname(__file__),
                                            '../../data/dataset_2006_elections.csv'),
                           2008: os.path.join(os.path.dirname(__file__),
                                            '../../data/dataset_2008_elections.csv'),
                           2010: os.path.join(os.path.dirname(__file__),
                                            '../../data/dataset_2010_elections.csv'),
                           2012: os.path.join(os.path.dirname(__file__),
                                            '../../data/dataset_2012_elections.csv'),
                           2014: os.path.join(os.path.dirname(__file__),
                                            '../../data/dataset_2014_elections.csv'),
                           2016: os.path.join(os.path.dirname(__file__),
                                            '../../data/dataset_2016_elections.csv'),
                           20018: os.path.join(os.path.dirname(__file__),
                                            '../../data/dataset_2018_elections.csv'),

                          }
    
    RANDOM_SEED = 42

    #------------------------------------
    # Constructor 
    #-------------------
    

    def __init__(self, label_col='VoterTurnout'):
        '''
        Constructor
        '''
        self.log = LoggingService()

        # Directory with various data files:
        self.data_dir = os.path.join(os.path.dirname(__file__), '../../data')
        
        # Initialize various mappings (State names to their abbrevs, etc.),
        self.import_state_mappings()
        
        # Import voter turnout:
        self.log.info("Importing voter turnout data (Election Project)...")
        voter_turnout = self.import_voter_turnout(self.VOTER_TURNOUT_FILES)
        self.log.info("Done importing voter turnout data (Election Project).")

        # Import csv file with Google query statistics:
        self.log.info("Importing Google search keyword counts (Google Trends)...")
        search_features = self.import_search_data(self.QUERY_TERM_FILES)
        self.log.info("Done importing Google search keyword counts (Google Trends).")

        # Join voter turnout and search frequencies 
        # into one wide table:
        self.log.info("Joining Google search with voter turnout...")
        election_features = self.merge_turnout_query_counts(voter_turnout, 
                                                            search_features)
        self.log.info("Done joining Google search with voter turnout.")
        
        # Resort the 2-tier index to keep performance, but
        # mostly to avoid the warning about performance degraded
        # due to unsorted multiindex:
        
        election_features = election_features.sort_index()
        
        # Add information about election time disasters:
        # Whether or not (col 'Disaster', and the name
        # of the disaster (col 'Disaster_Name'):

        self.log.info("Adding disaster history...")
        election_features = self.add_disaster_information(election_features)
        self.log.info("Done adding disaster history.")

        # Remove columns not needed, and encode 
        # categorical columns:
        
        # StateCode starts with consecutive ints
        # Must encode those:
        y = election_features[label_col]
        new_state_codes = self.leave_one_out_encode(cat_col=election_features['StateCode'],
                                                    label_col=y)
        election_features['StateCode'] = new_state_codes
        
        # Remove the ASCII State and disaster names:
        election_features = election_features.drop(columns=['State'])
        election_features = election_features.drop(columns=['DisasterName'])
        
        # Encode the Google query terms:
        queries_encoded = self.leave_one_out_encode(election_features['Query'], y)
        election_features['Query'] = queries_encoded
        
        # Same for the WeekDay col ('Mon', 'Tue',...):
        week_day_encoded = self.leave_one_out_encode(election_features['WeekDay'], y)
        election_features['WeekDay'] = week_day_encoded

        # Get (again, just in case) the values in column that we are to predict:
        y = election_features[label_col]
        # Remove that col from election_features:
        X = election_features.drop(columns=label_col)
        
        # Save the feature names (without the label col)
        # before turning X into an np array:
        self.feature_names = X.columns
        self.target_name   = label_col
        
        # Make the final feature vectors (incl. label_col)
        # available to other methods:
        self.election_features = election_features
        
        #**********
        self.explore_features()
        #**********
        
        # RandomForestClassivier/Regressor want
        # pure numpies:
        self.X = X.reset_index(drop=True).to_numpy(dtype=float)
        self.y = y.reset_index(drop=True).to_numpy(dtype=float)

        self.log.info("Creating RandomForestRegressor...")
#         self.rand_forest = RandomForestRegressor(n_estimators=1,
#                                                  max_depth=2,
#                                                  random_state=self.RANDOM_SEED
#                                                  )

        self.rand_forest = RandomForestRegressor()

        self.log.info("Done creating RandomForestRegressor.")

    #------------------------------------
    # run
    #-------------------

    def run(self):
        
        # Split the data into training and testing sets
        train_features, test_features, train_labels, test_labels = \
            train_test_split(self.X, self.y, test_size = 0.25, random_state = 42)

        self.optimize_hyperparameters(train_features, train_labels)

        self.log.info("Training the regressor...")
        self.rand_forest.fit(train_features, train_labels)
        self.log.info("Done training the regressor.")

        predictions = self.rand_forest.predict(test_features)
        self.evaluate_model(predictions, test_labels)

    #------------------------------------
    # optimize_hyperparameters 
    #-------------------
    
    def optimize_hyperparameters(self, train_X, train_y):
        
        tuned_parameters = {'n_estimators' : 1+np.array(range(10)),
                            'max_depth'    : 1+np.array(range(10))
                            }
        scorer = make_scorer(mean_squared_error, greater_is_better=False)
        clf = GridSearchCV(
            self.rand_forest,
            tuned_parameters,
            scoring=scorer,
            cv=5,
            n_jobs=10,
            verbose=1
            )
        clf.fit(train_X, 
                train_y
                )
        self.rand_forest = clf.best_estimator_
        
        importances = self.rand_forest.feature_importances_
        # Associate the importances with their feature
        # names as tuples (<feature_name>, <importance>):
        feature_importances = list(zip(self.feature_names,
                                       importances
                                       ))
        sorted_importances = sorted(feature_importances,
                                    key=lambda name_imp_pair: name_imp_pair[1],
                                    reverse=True
                                    )
        print(f'Feature importances: {sorted_importances}')
        
        #print(f'Best params: {clf.best_params_}')
        #print(f'Best score: {clf.best_score_}')

    #------------------------------------
    # explore_features 
    #-------------------
    
    def explore_features(self):
        
        #*****plt.figure(figsize=(1,1))
        plt.figure()
        cor = self.election_features.corr()
        sns.heatmap(cor, annot=True, cmap=plt.cm.Reds)
        plt.show()


    #------------------------------------
    # train_model 
    #-------------------
    
    def train_model(self):
        
        pass


    #------------------------------------
    # evaluate_model
    #-------------------

    def evaluate_model(self, predictions, test_labels):
        # Baseline model the mean of voter turnout:
        baseline_preds    = test_labels.mean()
        baseline_errors   = abs(baseline_preds - test_labels)
        mean_baseline_err = round(np.mean(baseline_errors), 2)

        self.log.info(f"Baseline mean abs err: {100*mean_baseline_err} percentage points.")

        rmse = round(mean_squared_error(test_labels,
                                        predictions,
                                        squared=False
                                        ),2)
        
        self.log.info(f'RMSE: {rmse}')

    #------------------------------------
    # import_search_data 
    #-------------------
    
    def import_search_data(self, search_data_dict):
        '''
        Read CSV file, and return a dataframe
        containing the feature vectors of Google
        searches for voting related terms 
        
        Assumptions:
           o First seven columns are query term counts for each
             day of the week.
           o Last column is label to predict (State in this
             case, but could be voter turnout)
             
             Example rows:
                 76,100,92,54,71,69,46,0,2008,vote     # record for week3 before election State 0
                 76,57,67,85,100,96,99,0,2008,vote     # record for week2 before election State 0
                 43,60,52,100,73,80,47,0,2008,vote     # record for week1 before election State 0
                 100,67,0,61,42,33,84,1,2008,vote      # record for week3 before election State 1
                 0,47,100,55,56,46,50,1,2008,vote      # record for week2 before election State 1
                 44,42,65,49,100,42,81,1,2008,vote     # record for week1 before election State 1
                 61,42,77,100,79,44,91,2,2008,vote                   ...       State 2

           o Consecutive rows are for weeks before election.
             Example: if data contains two weeks for each 
             label (State/voter turnout), then these rows
             represent data for two consecutive weeks before
             the election.
             
        Returns a dataframe, where '*' in the last three
        records is the sum of searches over all States on that day:
        
                State Query  Week0   Week1    Week2    Mon   Tue  Wed  Thu  Fri  Sat  Sun 
           ...    AL   vote    1       0        0       76  100   92   54   71   69   46
                  AL   vote    0       1        0       76   57   67   85  100   96   99
                  AL   vote    0       0        1       43   60   52  100   73   80   47
                  AK   vote    1       0        0      100   67    0   61   42   33   84
                    ...
                  US   vote    *   *    *    *    *    *    * 
                  US   vote    *   *    *    *    *    *    * 
                  US   vote    *   *    *    *    *    *    *

        In addition, a multiindex ('Region', 'Election') is installed,
        allowing selections such as:
        
        Ex1:   select all Wyoming data for the 2016 election:
           
            search_query_df.xs(['WY',2016])
            ==> Region Election                                                           
                WY     2016        WY  2016  vote     1   0   0  49    0    0  100   58   93   45
                       2016        WY  2016  vote     0   1   0  80    0   58   68   61  100    0
                       2016        WY  2016  vote     0   0   1  00    0    0   63    0   47   45
                       2016        WY  2016  vote     1   0   0  49    0    0  100   58   93   45
                       2016        WY  2016  vote     0   1   0  80    0   58   68   61  100    0
                       2016        WY  2016  vote     0   0   1  00    0    0   63    0   47   45
                       2016        WY  2016  vote     1   0   0  49    0    0  100   58   93   45
                       2016        WY  2016  vote     0   1   0  80    0   58   68   61  100    0
                       2016        WY  2016  vote     0   0   1  00    0    0   63    0   47   45

        Ex2.   select all vote counts of week 0
            search_query_df[search_query_df.Week == 0]
            ==> Region Election                                ...                              
                AL     2008        AL  2008  vote     1   0  0  46  ...    58    56    27    75   100
                AK     2008        AK  2008  vote     1   0  0   0  ...    81   100     0    76    74
                AZ     2008        AZ  2008  vote     1   0  0  91  ...    86    60    52    72    57
                AR     2008        AR  2008  vote     1   0  0  ...    90    96     0   100    67
                
        @param csv_file: comma separated data file 
        @type csv_file: str
        @return: tuple with matrix n x 8: one week indicator
            and seven day's of Google query search counts.
            Final column is state abbrev
        @rtype: (pd.DataFrame, pd.Series)
        '''

        search_query_df = None
        for year in search_data_dict.keys():
            
            csv_file = search_data_dict[year]
            df = pd.read_csv(csv_file,
                             names=['Mon','Tue','Wed','Thu','Fri','Sat','Sun',
                                    'StateCode', 'Query'],
                             index_col=False, # 1st col is *not* an index col
                             )
            
            # Add a column that indexes the week before
            # the election that one row represents.
            # A few steps needed:
            
            # How many weeks of data for each State?
            # Take State 0 (any will do), and could how
            # often it occurs in the State column:
            
            self.num_weeks  = len(df[df.StateCode == 0])
    
            # How many States are in the data? Find
            # by counting unique values in the States
            # column: 
            num_states = len(df.groupby('StateCode').nunique())

            # A new year col:
            df['Year'] = year

            # Create the Week col. Example for 3 weeks
            # of data:
            #     0
            # 0
            # 1
            # 2
            # 0
            # 1
            # 2
            # ... one triplet for each State:

            df['Week'] = [wk_idx for wk_idx in range(self.num_weeks)] * num_states

            # Build a row for each week that is
            # the sum of all queries on one day
            # across all States:
            #
            #    week   Mon                     Tue
            #      0  sum(of Mondays of wk0)    sum(of Tuesdays of wk0)    ...
            #      1  sum(of Mondays of wk1)    sum(of Tuesdays of wk0)    ...
            
            df_final = df.copy()
            for wk in range(self.num_weeks):
                that_wk_only = df[df['Week'] == wk]
                
                # Now sum all the weekday cols, 
                # getting a series [sum(Mon), sum(Tue),...]
                wk_summed = that_wk_only[['Mon','Tue','Wed','Thu','Fri','Sat','Sun']].sum(axis=0)
                
                # Add the StateCode, Query, and Week:
                wk_summed['StateCode']  = self.reverse_state_codings['US']
                wk_summed['Query']      = that_wk_only.loc[wk,'Query']
                wk_summed['Year']       = year
                wk_summed['Week']       = wk
    
                df_final = df_final.append(wk_summed, ignore_index=True)
                
            # Move the State column into left-most position:
            #df_final = df_final.reindex(columns=['State', 'Year', 'Query', 'Week', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
            
            # We now have columns:
            #   Index(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun', 'StateCode', 'Query', 'Week']

            if search_query_df is None:
                search_query_df = df_final.copy()
            else:
                search_query_df = search_query_df.append(df_final.copy())

        # Turn the weekday columns into two colums:
        #  'WeekDay' and 'DayCount'
        
        search_query_df_folded = self.fold_columns(search_query_df,
                                                   ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'], 
                                                   'WeekDay', 
                                                   'DayCount') 

        # We are returning the following columns:
        
        #['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun', 'StateCode', 'Query',
        # 'Year', 'Week']
        # 
        # And this multiindex:
        #
        # MultiIndex([('AL',  2004),
        #    ('AL',  2004),
        #    ('AL',  2004),
        #        ...
        #                ('WY', 20018),
        #    ('WY', 20018),
        #    ('US', 20018),
        #    ('US', 20018),
        #    ('US', 20018)],

        # Make a Region/Year multiindex:        
        # Get from the State code to the long State name to the State abbrev:

        state_codes   = search_query_df_folded['StateCode']
        idx_state = pd.Series([self.state_codings[state_code] for state_code in state_codes])
        idx_year  = search_query_df_folded['Year']
        idx_state.name = 'Region'
        idx_year.name = 'Election'
        new_idx = pd.MultiIndex.from_frame(pd.DataFrame({'Region' : idx_state.to_numpy(), 
                                                         'Election' : idx_year}
                                                         ))
        search_query_df_folded.index = new_idx
        return search_query_df_folded
    
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
           
           50  : 'WY'
           51  : 'US'    # <---- Added as if a State
              ...}

        2. A reverse dict: State --> StateCode:
           self.reverse_state_codings 
           
           {'AL'   :   0
            'AK'   :   1
           
            'WY'  : 50
            'US'  : 51    # <---- Added as if a State
              ...}


        3. self.state_abbrevs: long State names to 2-letter abbreviations
        
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
                
        # Add the US 'State' after the last real State:
        self.state_codings[max(self.state_codings.keys()) + 1] = 'US'
        
        # Reverse state/code lookup:
        self.reverse_state_codings = {}
        for state_code, state in self.state_codings.items():
            self.reverse_state_codings[state] = state_code

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
            
            # Some files have two extra columns: 'State Results Website',
            # 'Status', 'Source', and/or 'State Abv. Others don't.
            # Remove those cols if present:
            
            try:
                df = df.drop(columns=['State Results Website'], axis=1)
            except KeyError:
                pass
            try:
                df = df.drop(columns=['Status'], axis=1)
            except KeyError:
                pass
            try:
                df = df.drop(columns=['Source'], axis=1)
            except KeyError:
                pass
            try:
                df = df.drop('State Abv', axis=1)
            except KeyError:
                pass
            
            # Overseas Eligible Voters are only available
            # for the entire US; at State level the col is
            # all nan:
            
            df = df.drop('Overseas Eligible', axis=1)
            
            # Some of the Election Project sheets have 
            # Notes and such after Wyoming. Remove those:
            
            wyoming_idx_obj = df.index[(df.iloc[:,1] == 'Wyoming') == True]
            wyoming_idx     = wyoming_idx_obj.values[0]
            df = df[df.index <= wyoming_idx]
            
            # The 2018 table from Election Project is missing
            # the VAP Highest Office column. Get those numbers
            # via the Census:
            
            if year == 2018:

                vap_highest_office = self.compute_2018_VAPHighestOffice()
                # Get location of 'VEP Highest Offict', after which
                # we then place the data:
                dest = df.columns.get_loc('2018 Vote for Highest Office VEP Turnout Rate')
                df.insert(dest,'VAPHighestOffice',vap_highest_office.to_numpy())
            
            # 15 columns (each of which corresponds to a level in
            # the multiindex:
            try:
                df.columns = ['index_dup', 'State', 'VoterTurnout', 'VEPHighestOffice',
                       'VAPHighestOffice', 'TotalBallotsCounted', 'HighestOffice', 'VotingEligiblePopulation',
                       'VotingAgePopulation',
                       'NonCitizenPerc',
                       'Prison',
                       'Probation',
                       'Parole',
                       'TotalIneligibleFelons',
                       ]
            except Exception as e:
                # Without this try/except, program
                # just dies if mismatch in num of 
                # cols in df vs. the above list:
                raise e
    
            # Add a year col just after the State:
            df.insert(2,'Year',[year]*len(df)) 
            
            # Remove the index_dup col:
            df = df.drop(columns='index_dup')

            # We use 'VEP Total Ballots Counted' for voter participation. 
            # But some States don't report this number. Their value will 
            # be NaN. In that case we use the 'VEP Highest Office' percentage:
            
            df['VoterTurnout'] = df['VoterTurnout'].fillna(df['VEPHighestOffice'])
            
            # Same with 'Total Ballots Counted':
            df['TotalBallotsCounted'] = df['TotalBallotsCounted'].fillna(df['HighestOffice'])
    
            # The last row may have 'Notes' in it, delete such a row:
            #if df['State']['Unnamed: 0_level_1'].iloc[-1][-1].startswith('Note:'):
            #    us_plus_state_names = us_plus_state_names[:-1]['United States']
             
            # The State column may have row(s) that are notes.
            # We hope they'll continue to start with 'Note':
            df = df.drop(df[df.State.str.startswith('Note')].index)
            
            # 
            
            if voter_turnout_df is None:
                voter_turnout_df = df.copy()
            else:
                voter_turnout_df = voter_turnout_df.append(df.copy())

        # Set a two-element index to allow
        #   df.loc['CA', 2016] to get all 2016
        # California results. We get the data
        # for the two index levels from columns
        # 'State' and 'Year', but we rename them
        # to 'Region' and 'Election'
        
        state_abbrevs = self.state_abbrev_series(voter_turnout_df.State)
        region_series         = state_abbrevs
        election_series       = voter_turnout_df['Year'].copy()
        region_series.name    = 'Region'
        election_series.name  = 'Election'
        voter_turnout_df = voter_turnout_df.set_index([region_series, election_series])
        
        return voter_turnout_df

    #------------------------------------
    # add_disaster_information 
    #-------------------
    
    def add_disaster_information(self, election_features):
        
        # For convenience of var name brevity:
        df = election_features.copy()
        
        # Sort the index of the df so that we
        # don't get warnings about inefficiency 
        # when setting selected col values:
        df = df.sort_index()
        
        # Initialize the disaster and disaster name cols:
        df['Disaster'] = 0
        df['DisasterName'] = ''

        df.loc[('LA',2006), 'Disaster'] = 1
        df.loc[('LA',2006), 'DisasterName'] = 'Katrina'

        # Hurricane Sany, 2012:
        df.loc[('NY',2012), 'Disaster'] = 1
        df.loc[('NY',2012), 'DisasterName'] = 'Sandy'
        df.loc[('NJ',2012), 'Disaster'] = 1
        df.loc[('NJ',2012), 'DisasterName'] = 'Sandy'
        df.loc[('CT',2012), 'Disaster'] = 1
        df.loc[('CT',2012), 'DisasterName'] = 'Sandy'
        df.loc[('VA',2012), 'Disaster'] = 1
        df.loc[('VA',2012), 'DisasterName'] = 'Sandy'
        df.loc[('DE',2012), 'Disaster'] = 1
        df.loc[('DE',2012), 'DisasterName'] = 'Sandy'
        df.loc[('MA',2012), 'Disaster'] = 1
        df.loc[('MA',2012), 'DisasterName'] = 'Sandy'
        df.loc[('NH',2012), 'Disaster'] = 1
        df.loc[('NH',2012), 'DisasterName'] = 'Sandy'

        # Hurricane Matthew, 2016
        df.loc[('FL',2016), 'Disaster'] = 1
        df.loc[('FL',2016), 'DisasterName'] = 'Matthew'
        df.loc[('GA',2016), 'Disaster'] = 1
        df.loc[('GA',2016), 'DisasterName'] = 'Matthew'
        df.loc[('NC',2016), 'Disaster'] = 1
        df.loc[('NC',2016), 'DisasterName'] = 'Matthew'
        df.loc[('SC',2016), 'Disaster'] = 1
        df.loc[('SC',2016), 'DisasterName'] = 'Matthew'

        return df

    #------------------------------------
    # leave_one_out_encode 
    #-------------------
    
    def leave_one_out_encode(self, cat_col, label_col):
        '''
        Given a column with values for a categorical
        variable, and a target col (i.e. the col that is
        to be predicted), return a column with the 
        cat_col values leave-one-out encoded.
        
        @param cat_col: categorical values
        @type cat_col: pd.Series
        @param label_col: values to be predicted
        @type label_col: pd.Series
        @return leave-one-out encoded values
        @rtype: pd.Series
        '''

        # Sigma adds normal noise to the encodings to 
        # prevent overfitting:
        enc = LeaveOneOutEncoder(verbose=1,
                                 cols=cat_col.name,
                                 sigma=0.05)

        enc = enc.fit(cat_col, label_col)
        # The transform() method calls the encoder's
        # transform_leave_one_out() method:
        res = enc.transform(cat_col, label_col)
        
        return res

    #------------------------------------
    # merge_turnout_query_counts
    #-------------------

    def merge_turnout_query_counts(self, voter_turnout, search_features):
        '''
        Given a voter turnout dataframe, and a df containing
        search query counts, merge the two (i.e. add columns
        of one to the other), and return the result.
        
        @param voter_turnout: dataframe of all voter turnout data
        @type voter_turnout: pd.DataFrame
        @param search_features: dataframe of all voting related search query counts
            by state and election year
        @type search_features: pd.DataFrame
        @return a wider df, containing information from
            both inputs
        @rtype pd.DataFrame
        '''
        # Voter turnout and query frequencies
        # have two column names in common: State, and Year.
        # The rsuffix below renames the search_features
        # columns to State_xxx and Year_xxx in the 
        # join, so we can remove them later:
        
        features = voter_turnout.merge(search_features, 
                                       left_index=True,
                                       right_index=True,
                                       how='inner')

        # Rename/remove columns introduced by
        # the merge where column name ambiguities
        # existed:
        
        features = features.drop(labels=['Year_y'], axis='columns')
        features = features.rename({'Year_x' : 'Year'}, axis='columns')

        return features

# ------------------------ Utilities ----------

    #------------------------------------
    # fold_columns 
    #-------------------
    
    def fold_columns(self, df, cols, new_category_name, new_value_name):
        
        if type(cols) != list:
            cols = [cols]
        
        # The final df will be len(cols) times
        # the current len of df:
        
        non_involved_cols = [col for col in df.columns if col not in cols]
        new_cols = non_involved_cols.copy()
        new_cols.extend([new_category_name, new_value_name])
        
        res = pd.DataFrame([],columns=new_cols)
        
        for col in cols:
            new_col_values   = df[col]
            df_for_col = df[non_involved_cols]
            df_for_col.insert(len(df_for_col.columns),
                              new_category_name,
                              col)
            df_for_col.insert(len(df_for_col.columns),
                              new_value_name,
                              new_col_values)
            res = pd.concat([res, df_for_col])
            
        return res

    #------------------------------------
    # compute_2018_VAPHighestOffice
    #-------------------
    
    def compute_2018_VAPHighestOffice(self):
        '''
        The Election Project's 2018 speadsheet
        is missing the "VAP Highest Office" column.
        This method reads that year's turnout results
        taken from the Census at 
        https://www.census.gov/data/tables/time-series/demo/voting-and-registration/congressional-voting-tables.html
        Table 1.
        
        We read that table, aggregate over all Congressional
        districts to get State level numbers. Then compute the
        missing column, and return it as a pd.Series.
        
        @return one column with 2018 numbers for the 
            VAP Highest Office column.
        @rtype pd.Series
        '''
        turnout_file = os.path.join(self.data_dir, 'votingRatesCongressionalDistricts2018Corrected.xlsx')
        df = pd.read_excel(turnout_file, header=[0], index_col=0)
        df_by_state = df.groupby('StateAbbreviation').sum()
        # Don't need the margins of error:
        voting_rate_VAP = 100 * df_by_state['VotesCast'] / df_by_state['VotingAgePopVAP']

        # Compute the nationwide mean:
        us_mean = voting_rate_VAP.mean()
        voting_rate_VAP = pd.concat([pd.Series(us_mean, index=['US']),voting_rate_VAP])
        
        return voting_rate_VAP

    #------------------------------------
    # state_abbrev_series 
    #-------------------

    def state_abbrev_series(self, state_name_series):
        # One 'state' name in the voter turnout Excel
        # sheets is 'United States', leave that abbrev
        # NaN to conform with the newer turnout sheets:
        
        res = state_name_series.apply(lambda state_nm: 'US' \
                                      if state_nm == 'United States' else self.state_abbrevs[state_nm])
        return res

# ------------------------ Main ------------
if __name__ == '__main__':
    
#     parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
#                                      formatter_class=argparse.RawTextHelpFormatter,
#                                      description="Predict State from Google queries"
#                                      )
# 
#     parser.add_argument('-l', '--errLogFile',
#                         help='fully qualified log file name to which info and error messages \n' +\
#                              'are directed. Default: stdout.',
#                         dest='errLogFile',
#                         default=None);
# 
#     args = parser.parse_args();

    StatePredictor().run()