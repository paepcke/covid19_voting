#!/usr/bin/env python3
'''
Created on Sep 30, 2020

@author: paepcke
'''
import argparse
import os
from pathlib import Path
import sys

from sklearn.base import BaseEstimator, TransformerMixin

import numpy as np
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '.'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from spreadsheet_colname_mappings import spreadsheet_maps
from utils.logging_service import LoggingService

def to_int_if_possible(one_row_series):
    # The FIPSCode looks like an int, but must
    # be treated like a str to preserve leading
    # zeroes. This is done during rexad_excel():
    if one_row_series.name == 'FIPSCode':
        return one_row_series
    return one_row_series.apply(catch_non_int)
    
def catch_non_int(value):
    try:
        int_val = int(value)
        # Some code 'not available' with negative 
        # nums, like -99999999:
        return max(int_val, 0)
    except ValueError:
        # Detect does not apply, etc.
        if type(value) == str and \
            (value.startswith('Does not apply') or \
             value.startswith('Data not available') or \
             value.startswith('-999') or \
             value.startswith('-888')):
            return 0
        else:
            # Probably a string, like a State name:
            return value


class ElectionSurveyCleaner(BaseEstimator, TransformerMixin):
    '''
    classdocs
    '''

    EAVS_FILES =         {
                            2014: os.path.join(os.path.dirname(__file__),
                                               '../../data/Exploration/inAbsentia2014.xlsx'),
                            2016: os.path.join(os.path.dirname(__file__),
                                               '../../data/Exploration/inAbsentia2016.xlsx'),
                            2018: os.path.join(os.path.dirname(__file__),
                                               '../../data/Exploration/EAVS_2018_for_Public_Release_Updates3.xlsx')
                                               #'../../data/Exploration/inAbsentia2018.xlsx')
                            }

    #------------------------------------
    # Constructor 
    #-------------------

    def __init__(self):
        '''
        Constructor
        '''
        self.log = LoggingService()
        
        # Place to collect aggregrations
        # Will be one df with multiindex 
        #   FIPSCodeDetailed,
        # 
        # Columns will be the union of the percentages
        # of the years, each starting with {year}.
        # One row per county.
        
        self.percentages = pd.DataFrame()
        
        state_fips_file = os.path.join(os.path.dirname(__file__),
                                       '../../data/Exploration/fips_states_only.xlsx')
        
        # Import the Census FIPS codes for all years:
        self.load_census_geocodes()
        
        # Initialize a simple table 
        #     StateFull,  StateFIPS,   State
        #    'Wisconsin',    '55',       'WI'
        #                              ...
        self.log.info("Reading State FIPS codes...")
        
        self.state_fips = pd.read_excel(state_fips_file,
                                        header=[0],
                                        dtype={
                                             'StateFull' : str,
                                             'StateFIPS' : str,
                                             'State' : str
                                             }
                                        )
        # All entries must be strings:
        self.state_fips = self.state_fips.astype(str)
        # The State FIPS must have two digits:
        self.state_fips.StateFIPS = self.state_fips.StateFIPS.str.zfill(2)

        self.log.info("Done reading State FIPS codes.")

    #------------------------------------
    # load_census_geocodes
    #-------------------
    
    def load_census_geocodes(self):
        '''
        Loads and if necessary corrects the
        Census bureau FIPS files in force in
        2018, 2016, 2014. Initializes
        
           self.geocodes{year}
        '''

        self.geocodes = {}
        
        #*********
        #for year in [2014, 2016, 2018]:
        for year in [2018]:
        #*********
            # Get reference to the Census geo codes:
            geocode_file = os.path.join(os.path.dirname(__file__),
                                        f'../../data/Exploration/all-geocodes-v{year}.xlsx')
    
            self.log.info(f"Reading Census geo codes of year {year}...")
            self.geocodes[year] = pd.read_excel(geocode_file,
                                                skiprows=[0,1,2,3],
                                                header=[0],
                                                usecols=['State Code (FIPS)',
                                                         'County Code (FIPS)',
                                                         'County Subdivision Code (FIPS)',
                                                         'Area Name (including legal/statistical area description)'
                                                         ],
                                                dtype={
                                                       'State Code (FIPS)' : str,
                                                       'County Code (FIPS)' : str,
                                                       'County Subdivision Code (FIPS)' : str,
                                                       'Area Name (including legal/statistical area description)': str
                                                       }
                                                )
            self.log.info(f"Done reading Census geo codes of year {year}.")
            
            # Shorten the col names:
            self.geocodes[year] = self.geocodes[year].rename({
                'State Code (FIPS)' : 'StateFIPS',
                'County Code (FIPS)': 'County',
                'County Subdivision Code (FIPS)' : 'Subdivision',
                'Area Name (including legal/statistical area description)' : 'Jurisdiction'
                }, axis=1)
            # Ensure that all subdivision codes have
            # leading zeroes to make them all length 5:
            self.geocodes[year]['Subdivision'] = self.geocodes[year]['Subdivision'].str.zfill(5)
            # Remove Puerto Rico rows:
            self.geocodes[year] = self.geocodes[year].drop(self.geocodes[year][self.geocodes[year]['StateFIPS'] == '72'].index, 
                                               axis=0)

        # Fixes for 2018:
        # ---------------
        # Fix missing entry for one Wisconsin county: 
        #   ['55','101','89550','Racine County']
        new_entry = pd.DataFrame({'StateFIPS'    : '55',
                                  'County'       : '101',
                                  'Subdivision'  : '89550',
                                  'Jurisdiction' : 'Racine County'
                                  }, index=[len(self.geocodes[year])])
        self.geocodes[2018] = pd.concat([self.geocodes[2018], new_entry], axis=0)
           
        # Fix missing Maine abroad entry:
        new_entry = pd.DataFrame({'StateFIPS'   : '23',
                                  'County'      : '000',
                                  'Subdivision' : '23',
                                  'Jurisdiction': 'MAINE - UOCAVA'
                                  }, index=[len(self.geocodes)])
        self.geocodes[2018] = pd.concat([self.geocodes[2018], new_entry], axis=0)
 
#*************
#         # Fixes for 2016:
#         # ---------------
#          
#         new_entry = pd.DataFrame({'StateFIPS'    : '55',
#                                   'County'       : '139',
#                                   'Subdivision'  : '26982',
#                                   'Jurisdiction' : 'Winnebago County'
#                                   }, index=[len(self.geocodes[year])])
#         self.geocodes[2016] = pd.concat([self.geocodes[2016], new_entry], axis=0)
#    
#         # Fix missing Fox Crossing village:
#         new_entry = pd.DataFrame({'StateFIPS'   : '55',
#                                   'County'      : '139',
#                                   'Subdivision' : '0000',
#                                   'Jurisdiction': 'Fox Crossing village'
#                                   }, index=[len(self.geocodes)])
#    
#         self.geocodes[2016] = pd.concat([self.geocodes[2016], new_entry], axis=0)
#*************

    #------------------------------------
    # clean_survey_2018 
    #-------------------
    
    def clean_survey_2018(self, survey_file):
        
        year = 2018
        self.log.info(f"Reading Election Administration and Voting Survey for {year}...")
        sheet = pd.read_excel(io=survey_file,
                              header=[0],
                              usecols=spreadsheet_maps[year],
                              dtype={'FIPSCode' : str})

        self.log.info(f"Done reading Election Administration and Voting Survey for {year}.")
        # Sometimes a col 'PreferredOrder sneaks in:
        try:
            sheet = sheet.drop('PreferredOrder', axis=1)
        except KeyError:
            pass

        # Turn numbers to ints, except for FIPS Code:
        sheet = sheet.apply(to_int_if_possible, axis=0, result_type='broadcast')

        df = sheet.rename(spreadsheet_maps[year], axis=1)
            
        # There can be stray columns where
        # all values are NaN (their names are
        # like 'Unnamed : 47'
        df = df.fillna(0)
        
        # Despite efforts in to_int_if_possible(),
        # must turn FIPS col into strings manually:
        df['FIPSCodeDetailed'] = df['FIPSCodeDetailed'].astype(str)
#         # Make 10-digit FIPSCode into str, b/c the leading
#         # zeroes are confused with Octal:
#         df = df.astype({f'FIPSCodeDetailed' : str})
        
        # The State abbreviation needs to be right after
        # the detailed FIPS code to be uniform with other
        # sheets:
        
        st_abbr = df[f'{year}State_Abbr']
        df = df.drop(f'{year}State_Abbr', axis=1)
        df.insert(1,f'State', st_abbr)

        # Remove Guam, Virgin Islands, AMERICAN SAMOA, Puerto Rico:
        df = df.drop(df[df['State'] == 'GU'].index)
        df = df.drop(df[df['State'] == 'VI'].index)
        df = df.drop(df[df['State'] == 'AS'].index)
        df = df.drop(df[df['State'] == 'PR'].index)

        # Create the 5-digit FIPS version, where the
        # first two digits designate the State, and
        # remaining 3 denote the county:

        countyFIPS = self.parse_fips_codes(df[[f'FIPSCodeDetailed', 
                                               'State',
                                               'Jurisdiction']],
                                              self.geocodes[year])


        df_new = df.merge(countyFIPS, on='FIPSCodeDetailed')

        # Move the county FIPS col to after the Jurisdiction:
        # Get all the cols other than {year}FIPSCounty, which
        # the merge put at the end:

        cols = list(df_new.columns.values)
        county_col_name = cols.pop()
        juris_pos = 1+cols.index('Jurisdiction')
        head = cols[:juris_pos]
        tail = cols[juris_pos:]
        new_cols = head + [county_col_name] + tail
        df = df_new[new_cols]
        
        # A very occasional NaN slips in here (One in 2018)
        # Delete respective row(s):
        df = df.dropna(axis=0) 
        
        # Rename the FIPS col to conform to our norms:
        df = df.rename({county_col_name : f'{year}CountyFIPS'}, axis=1)

        # Remove all rows with three zeroes in the County FIPS.
        # Those each denote States overall: 01000:Alabama, etc.
        df = df.drop(df[df[f'{year}CountyFIPS'].str.endswith('000')].index)

        # Make multiindex:
        mindx_df = df[['FIPSCodeDetailed','State','Jurisdiction']].copy()
        mindx_df.columns = ['FIPSDetailed','State','Jurisdiction']

        mindx_df['Election'] = [year]*len(df)
        mindx = pd.MultiIndex.from_frame(mindx_df,
                                         names=['FIPSDetailed',
                                                'State',
                                                'Jurisdiction',
                                                'Election'
                                                ])
        df.index = mindx
        
        # Remove the now superfluous cols:
        df_final = df.drop(['FIPSCodeDetailed', 'State', 'Jurisdiction'], axis=1)
        
        # Several fields are incorrect in the original. Fix some of those:
        
#         self.set_at(df_final,'AZ', 'NAVAJO COUNTY',f'{year}ByMailCountByMailRejected',211)
#         self.set_at(df_final,'AZ', 'PINAL COUNTY',f'{year}ByMailCountByMailRejected',339)
#         self.set_at(df_final,'FL', 'SARASOTA COUNTY',f'{year}ByMailCountByMailRejected',380)
#         self.set_at(df_final,'FL', 'ST. LUCIE COUNTY',f'{year}ByMailCountByMailRejected',694)
#         self.set_at(df_final,'TX', 'DALLAM COUNTY',f'{year}ByMailCountByMailRejected',7)
#         self.set_at(df_final,'TX', 'LAMPASAS COUNTY',f'{year}ByMailCountByMailRejected',4 )
#         self.set_at(df_final,'TX', 'CASTRO COUNTY',f'{year}ByMailCountByMailRejected', 6)
#         self.set_at(df_final,'TX', 'CHILDRESS COUNTY',f'{year}ByMailCountByMailRejected',2 )
#         self.set_at(df_final,'TX', 'CULBERSON COUNTY',f'{year}ByMailCountByMailRejected',8)
#         self.set_at(df_final,'TX', 'HUNT COUNTY',f'{year}ByMailCountByMailRejected',42)
#         self.set_at(df_final,'TX', 'IRION COUNTY',f'{year}ByMailCountByMailRejected',1)
#         self.set_at(df_final,'TX', 'MARTIN COUNTY',f'{year}ByMailCountByMailRejected',1)
#         self.set_at(df_final,'TX', 'ROBERTS COUNTY',f'{year}ByMailCountByMailRejected',1)
#         self.set_at(df_final,'TX', 'STERLING COUNTY',f'{year}ByMailCountByMailRejected',5)

        # In 1130 rows the F1a total sum of votes cast
        # by all the possible modalities is not the
        # sum of F1b-F1g. Correct those rows:
    
        df_totals = df_final[[f'{year}TotalVotedPhysically',           
                              f'{year}TotalVotedAbroad',               
                              f'{year}TotalVoteByMail',                
                              f'{year}TotalVoteProvisionalBallot',     
                              f'{year}TotalVoteInPersonEarly',         
                              f'{year}TotalVoteByMailOnlyJurisdiction',
                              f'{year}TotalVoteOtherCount'
                              ]]
        sums_of_vote_modalities = df_totals.sum(axis=1, numeric_only=True)
        num_bad_modalities = (sums_of_vote_modalities != df_final[f'{year}TotalVoteCounted']).sum()
        self.log.info(f"Number of 'Sum of vote modalities != total vote' (sum(F1b-g) != F1a): {num_bad_modalities}")

        df_final[f'{year}TotalVoteCounted'] = sums_of_vote_modalities
        
        # Same for reasons for provisional votes rejection: in some counties 
        # the reasons don't add up to the total provisional rejections:
        # Same for rejections of provisional ballots:
        df_totals = df_final[[f'{year}ProvisionalRejProvisionalNotRegistered',
                              f'{year}ProvisionalRejWrongJurisdiction',       
                              f'{year}ProvisionalRejWrongPrecinct',           
                              f'{year}ProvisionalRejNoID',                    
                              f'{year}ProvisionalRejIncomplete',              
                              f'{year}ProvisionalRejBallotMissing',           
                              f'{year}ProvisionalRejNoSig',                   
                              f'{year}ProvisionalRejSigNotMatching',          
                              f'{year}ProvisionalRejAlreadyVoted',
                              f'{year}ProvisionalRejOther1Txt',  
                              f'{year}ProvisionalRej1Count',     
                              f'{year}ProvisionalRejOther2Txt',  
                              f'{year}ProvisionalRejOther2Count',
                              f'{year}ProvisionalRejOther3Txt',  
                              f'{year}RejProvisionalOther3Count'
                              ]]
                              
        sums_prov_rej = df_totals.sum(axis=1, numeric_only=True)
        num_bad_prov_rej = (sums_prov_rej != df_final[f'{year}ProvisionalRejCountTotal']).sum()
        self.log.info(f"Number of 'Sum of rejected provisionals != total num of provisionals' (sum(E2b-j) != E2a): {num_bad_prov_rej}")
        
        df_final[f'{year}ProvisionalRejCountTotal'] = sums_prov_rej
        
        # Same for rejections of vote by mail:

        df_totals = df_final[[f'{year}ByMailRejDeadline',
                              f'{year}ByMailRejSignatureMissing',
                              f'{year}ByMailRejWitnessSignature',
                              f'{year}ByMailRejNonMatchingSig',
                              f'{year}ByMailRejNoElectionOfficialSig',
                              f'{year}ByMailRejUnofficialEnvelope',
                              f'{year}ByMailRejBallotMissing',
                              f'{year}ByMailRejEnvelopeNotSealed',
                              f'{year}ByMailRejNoAddr',
                              f'{year}ByMailRejMultipleBallots',
                              f'{year}ByMailRejDeceased',
                              f'{year}ByMailRejAlreadyVoted',
                              f'{year}ByMailRejNoVoterId',
                              f'{year}ByMailRejNoBallotApplication',
                              f'{year}ByMailRejOtherReason1',
                              f'{year}ByMailRejOtherReasonCount1',
                              f'{year}ByMailRejOtherReason2',
                              f'{year}ByMailRejOtherReason2Count',
                              f'{year}ByMailRejOtherReason3',
                              f'{year}ByMailRejOtherReason3Count'
                              ]]

        sums_mail_rej = df_totals.sum(axis=1, numeric_only=True)
        num_bad_mail_rej = (sums_mail_rej != df_final[f'{year}ByMailCountByMailRejected']).sum()
        self.log.info(f"Number of 'Sum of rejected mail-ins != total num of mail-ins' (sum(C4b-o) != C4a): {num_bad_mail_rej}")
        
        df_final[f'{year}ByMailCountByMailRejected'] = sums_mail_rej


        # Still bad:
        # (df_perc > 100).sum() > 0
        # 2018PercByMailRejDeadline                  True
        # 2018PercByMailRejAlreadyVoted              True
        # 2018PercByProvRejNotRegistered             True
        # 2018PercByProvRejWrongJurisdiction         True
        # 2018PercByProvRejIncomplete                True
        # 2018PercVoteModusByMail                    True
        # 2018PercVoteModusInPersonEarly             True

        
        # Fill in the percentage calculations in
        # self.percentages for 2018
        self.percentages = self.compute_percentages_2018(df_final, year)
        
        return df_final

    #------------------------------------
    # set_at 
    #-------------------
    
    def set_at(self, df, state, jurisdiction, col_name, value):
        '''
        Set a particular cell's value in a survey df in place.
        Returns the changed df
        
        Ex.: 
            self.set_at(df, 'AZ', 'NAVAJO COUNTY', '2018ByMailCountByMailRejected', 300.9)
            
        There should be an elegant method to do this,
        but with out 4-level multiindex it's infernal.
        xs() won't work, b/c it returns a view. And at()/loc()
        don't work either, likely b/c we never know the 
        outermost (FIPSDetailed) value.
        
        @param df: EAV survey
        @type df: pd.DataFrame
        @param state: 2-letter State abbreviation
        @type state: str
        @param jurisdiction: name of election jurisdiction
            (often a County, but not always.)
        @type jurisdiction: str
        @param col_name: name of column where the
            target cell resides
        @type col_name: str
        @param value: value to set
        @type value: <any>
        @returns the newly changed df
        @rtype pd.DataFrame
        '''
        
        idx = df.xs([state, jurisdiction], level=['State','Jurisdiction'], drop_level= False).index
        df.at[idx.item(),col_name] = value
        return df

    #------------------------------------
    # clean_survey_2016 
    #-------------------
    
    def clean_survey_2016(self, survey_file):

        year = 2016
        
        self.log.info(f"Reading Election Administration and Voting Survey for {year}...")
        # The FIPS codes are 10 digits:
        #   State (2), County (3), Subdivision (5).
        # To maintain exactly 10 digits, read as
        # string:
        sheet = pd.read_excel(io=survey_file,
                              header=[0],
                              dtype={'FIPSCode' : str})

        # Turn numbers to ints, except for FIPS Code:
        sheet = sheet.apply(to_int_if_possible, axis=0, result_type='broadcast')

        self.log.info(f"Done reading Election Administration and Voting Survey for {year}.")
        df = sheet.rename({
                           'FIPSCode'                                 : f'FIPSCodeDetailed',
                           'JurisdictionName'                         : f'Jurisdiction',
                           'F1aVoterTurnout'                          : f'{year}TotalVote',
                           'F1bVoteAtPhysical'                        : f'{year}TotalVoteAtPhysicalCenter',
                           'F1cVoteAbroad'                            : f'{year}TotalVoteAbroad',
                           'F1dVoteAbsentee'                          : f'{year}TotalVoteAbsentee',
                           'F1eVoteProvisional'                       : f'{year}TotalVoteProvisional',
                           'F1fVoteEarlyPhysical'                     : f'{year}TotalVoteEarlyBallotCenters',
                           'F1gVoteByMail'                            : f'{year}TotalByMail',
                           'F3VoteFirstTimersIDNeededAndProvided'     : f'{year}TotalVoteNeededAndHadID',
                           'E1aProvisionalTotal'                      : f'{year}TotalProvisional',
                           'E1bProvisionalCountedFull'                : f'{year}TotalProvisionalCountedFull',
                           'E1cProvisionalCountedPartially'           : f'{year}TotalProvisionalCountedPartial',
                           'E1dProvisionalRejectedTotal'              : f'{year}TotalsionalRejected',
                           'E2aProvisionalRejVoterNotInState'         : f'{year}ProvisionalRejNotInState',
                           'E2bProvisionalRejWrongJurisdiction'       : f'{year}ProvisionalRejWrongJurisdiction',
                           'E2cProvisionalRejWrongPrecinct'           : f'{year}ProvisionalRejWrongPrecinct',
                           'E2dProvisionalRejInsufficientID'          : f'{year}ProvisionalRejInsufficientId',
                           'E2eProvisionalRejIncompleteOrIllegible'   : f'{year}ProvisionalRejIllegible',
                           'E2fProvisionalRejBallotMissing'           : f'{year}ProvisionalRejBallotMissing',
                           'E2gProvisionalRejNoSig'                   : f'{year}ProvisionalRejNoSig',
                           'E2hProvisionalRejNonMatchingSig'          : f'{year}ProvisionalRejSigNotMatching',
                           'E2iProvisionalRejAlreadyVoted'            : f'{year}ProvisionalRejAlreadyVoted',
                           'E2j_OtherProvisionalRejOther'             : f'{year}ProvisionalRejOther',
                           'C1aAbsenteeSent'                          : f'{year}TotalAbsenteeReturnedForCounting',
                           'C1bAbsenteeReturnedForCounting'           : f'{year}TotalVoteAbsenteeReturned',
                           'C1cAbsenteeReturnedUndeliverable'         : f'{year}TotalAbsenteeUndeliverable',
                           'C1dAbsenteeSpoiled'                       : f'{year}AbsenteeRejSpoiled',
                           'C2AbsenteePermanentList?'                 : f'{year}OperationsVoteAbsenteePermanentListYesNo',
                           'C3AbsenteeSentBecauseOnPermanentList'     : f'{year}TotalAbsenteeVotedFromPermanentListCount',
                           'C4aAbsenteeCounted'                       : f'{year}TotalAbsenteeCounted',
                           'C4bAbsenteeRejected'                      : f'{year}TotalAbsenteeRej',
                           'C5aAbsenteeRejLate'                       : f'{year}AbsenteeRejLate',
                           'C5bAbsenteeRejNoSig'                      : f'{year}AbsenteeRejNoSig',
                           'C5cAbsenteeRejNoWitnessSig'               : f'{year}AbsenteeRejNoWitnessSig',
                           'C5dAbsenteeRejSigNotMatching'             : f'{year}AbsenteeRejSigNotMatching',
                           'C5eAbsenteeRejNoElectOfficialSig'         : f'{year}AbsenteeRejNoElectionOfficialSig',
                           'C5fAbsenteeRejUnofficialEnvelope'         : f'{year}AbsenteeRejNonOfficialEnvelope',
                           'C5gAbsenteeRejBallotMissing'              : f'{year}AbsenteeRejBallotMissing',
                           'C5hAbsenteeRejEnvNotSealed'               : f'{year}AbsenteeRejEnvNotSealed',
                           'C5iAbsenteeRejNoResidentAddr'             : f'{year}AbsenteeRejNoResidentAddr',
                           'C5jAbsenteeRejMultipleBallots'            : f'{year}AbsenteeRejMultipleBallotsInEnv',
                           'C5kAbsenteeRejVoterDeceased'              : f'{year}AbsenteeRejVoterDeceased',
                           'C5lAbsenteeRejAlreadyVoted'               : f'{year}AbsenteeRejAlreadyVoted',
                           'C5mAbsenteeRejFirstTimeVoterBadID'        : f'{year}AbsenteeRejBadId',
                           'C5nAbsenteeRejNoApplication'              : f'{year}AbsenteeRejNoApplication',
                           'D1aNumPrecincts'                          : f'{year}OperationsNumPrecincts',
                           'D1CommentsPrecincts'                      : f'{year}OperationsNumPrecinctsComments',
                           'D2aNumPollingPlaces'                      : f'{year}OperationsNumPollingPlaces',
                           'D3aNumPollingWorkers'                     : f'{year}OperationsNumPollWorkers',
                           'D4aPWUnder18'                             : f'{year}OperationsPWUnder18',
                           'D4bPW18_25'                               : f'{year}OperationsPW18_25',
                           'D4c26_40'                                 : f'{year}OperationsPW26_40',
                           'D4dPW41_60'                               : f'{year}OperationsPW41_60',
                           'D4ePW61_70'                               : f'{year}OperationsPW61_70',
                           'D4fPWOver70'                              : f'{year}OperationsPW71Plus',
                           'D5PWRecruitingDifficulty'                 : f'{year}OperationsPWRecruitingDifficulty',
                           'D5CommentsPW'                             : f'{year}OperationsPWDifficultyComments'
                         }, axis=1)

        # There can be stray columns where
        # all values are NaN (their names are
        # like 'Unnamed : 47'
        df = df.loc[:,~(df.isna().all())]
        
        # Remove 'not applicables' and 'not availables' that
        # are expressed as negative integers:
        df = df.replace(to_replace={-888888    : 0,
                                    -999999    : 0,
                                    -9999999   : 0,
                                    '-888888.' : 0
                                    })
        
        # Replace the "23." in FIPSCodeDetailed with '23':
        df = df.replace(to_replace={'FIPSCodeDetailed' : '23.'}, value='23')
        
        # Remove Guam, Virgin Islands, AMERICAN SAMOA, Puerto Rico:
        df = df.drop(df[df['State'] == 'GU'].index)
        df = df.drop(df[df['State'] == 'VI'].index)
        df = df.drop(df[df['State'] == 'AS'].index)
        df = df.drop(df[df['State'] == 'PR'].index)

        # Create the 5-digit FIPS version, where the
        # first two digits designate the State, and
        # remaining 3 denote the county:

        countyFIPS = self.parse_fips_codes(df[[f'FIPSCodeDetailed', 
                                               'State',
                                               'Jurisdiction']],
                                              self.geocodes[year])


        df_new = df.merge(countyFIPS, on='FIPSCodeDetailed')

        # Move the county FIPS col to after the Jurisdiction:
        # Get all the cols other than {year}FIPSCounty, which
        # the merge put at the end:

        cols = list(df_new.columns.values)
        county_col_name = cols.pop()
        juris_pos = 1+cols.index('Jurisdiction')
        head = cols[:juris_pos]
        tail = cols[juris_pos:]
        new_cols = head + [county_col_name] + tail
        df = df_new[new_cols]
        
        # Rename the FIPS col to conform to our norms:
        df = df.rename({county_col_name : f'{year}CountyFIPS'}, axis=1)
        
        # Make *all* the FIPSDetailed values be 10 digits
        # by replacing the ones that have fewer with the 
        # FIPS County codes followed by zeroes:
        
        # Make multiindex:
        mindx_df = df[['FIPSCodeDetailed','State','Jurisdiction']].rename({'FIPSCodeDetailed' : 'FIPSDetailed'}, axis=1)
        mindx_df['Election'] = [year]*len(df)
        mindx = pd.MultiIndex.from_frame(mindx_df,
                                         names=['FIPSDetailed',
                                                'State',
                                                'Jurisdiction',
                                                'Election'
                                                ])
        df.index = mindx
        # Remove the now superfluous cols:
        df_final = df.drop(['FIPSCodeDetailed', 'State', 'Jurisdiction'], axis=1)

        return df_final


#     #------------------------------------
#     # ensure_10dig_key
#     #-------------------
#     
#     def ensure_10dig_key(self, df, countyFIPS):
#         '''
#         Ensures that the FIPSDetailed index consists
#         of unique, 10-digit numbers (as strings).
#         Needed mostly because of Wisconsin, which reports
#         with 5 or fewer digits. The countyFIPS is
#         expected to be a pd series as long as
#         df is high. For current FIPSDetailed with fewer
#         than 10 digits, the method prepends the 5-dig
#         county FIPS, keeps the rest, and fills with
#         '0' if needed.
# 
#         @param df: EAV survey
#         @type df: pd.DataFrame
#         @param countyFIPS: county FIPS for each row
#         @type countyFIPS: pd.Series
#         @return: new df with all FIPSDetailed unique,
#             and 10 chars wide.
#         @rtype: pd.DataFrame
#         '''
#         
#         df.index['FIPSDetailed']

        

    #------------------------------------
    # clean_survey_2014
    #-------------------
    
    def clean_survey_2014(self, survey_file):
        
        year = 2014
        self.log.info(f"Reading Election Administration and Voting Survey for {year}...")
        sheet = pd.read_excel(io=survey_file,
                              header=[0], dtype={'FIPSCode' : str})

        self.log.info(f"Done reading Election Administration and Voting Survey for {year}.")
        
        # Turn numbers to ints, except for FIPS Code:
        sheet = sheet.apply(to_int_if_possible, axis=0, result_type='broadcast')

        # Sometimes a col 'PreferredOrder sneaks in:
        try:
            sheet = sheet.drop('PreferredOrder', axis=1)
        except KeyError:
            pass
        
        df = sheet.rename({
                'FIPSCode' :                                         f'FIPSCodeDetailed',
                'PreferredOrder' :                                   f'{year}PreferredOrder',
                'State' :                                            f'State',
                'Jurisdiction' :                                     f'Jurisdiction',
                'QF1aVoteTotalCount' :                               f'{year}TotalCountVote',
                'QF1bVoteNumInPhysicalLoc' :                         f'{year}TotalVoteNumInPhysicalLoc',
                'QF1cVoteAbroad' :                                   f'{year}TotalVoteAbroad',
                'QF1dVoteAbsentee' :                                 f'{year}TotalVoteAbsentee',
                'QF1eVoteProvisional' :                              f'{year}TotalVoteProvisional',
                'QF1fVoteAtEarlyVoteCenter' :                        f'{year}TotalVoteAtEarlyVoteCenter',
                'QF1gVoteByMail' :                                   f'{year}TotalVoteByMail',
                'QF3VoteProvidedID' :                                f'{year}TotalVoteProvidedID',
                'QF3_CommentVote' :                                  f'{year}CommentsVote',
                'QC1aNumAbsenteeBallotsTransmitted' :                f'{year}TotalNumAbsenteeBallotsTransmitted',
                'QC1bAbsenteeSentInForCounting' :                    f'{year}TotalAbsenteeSentInForCounting',
                'QC1dAbsenteeSpoiled' :                              f'{year}TotalAbsenteeSpoiled',
                'QC1eAbsenteeNotReturnedByVoter' :                   f'{year}TotalAbsenteeNotReturnedByVoter',
                'QC2AbsenteeHavePermanentListAutoSend' :             f'{year}OperationsAbsenteeHavePermanentListAutoSend',
                'QC3AbsenteeNumSentFromPermanentList' :              f'{year}TotalAbsenteeNumSentFromPermanentList',
                'QC4aAbsenteeTotalCounted' :                         f'{year}TotalAbsenteeTotalCounted',
                'QC4bAbsenteeNumRejected' :                          f'{year}TotalAbsenteeNumRejected',
                'QC5aAbsenteeRejDeadline' :                          f'{year}AbsenteeRejDeadline',
                'QC5bAbsenteeRejNoVoterSig' :                        f'{year}AbsenteeRejNoVoterSig',
                'QC5cAbsenteeRejNoWitnessSig' :                      f'{year}AbsenteeRejNoWitnessSig',
                'QC5dAbsenteeRejNonMatchingSig' :                    f'{year}AbsenteeRejNonMatchingSig',
                'QC5eAbsenteeRejNoElectionOfficialSig' :             f'{year}AbsenteeRejNoElectionOfficialSig',
                'QC5fAbsenteeRejUnofficialEnvelope' :                f'{year}AbsenteeRejUnofficialEnvelope',
                'QC5gAbsenteeRejBallotMissing' :                     f'{year}AbsenteeRejBallotMissing',
                'QC5hAbsenteeRejEnvelopeNotSealed' :                 f'{year}AbsenteeRejEnvelopeNotSealed',
                'QC5iAbsenteeRejNoResidentAddr' :                    f'{year}AbsenteeRejNoResidentAddr',
                'QC5jAbsenteeRejMultipleBallotsInEnvelope' :         f'{year}AbsenteeRejMultipleBallotsInEnvelope',
                'QC5kAbsenteeRejVoterDeceased' :                     f'{year}AbsenteeRejVoterDeceased',
                'QC5lAbsenteeRejAlreadyVoted' :                      f'{year}AbsenteeRejAlreadyVoted',
                'QC5mAbsenteeRejFirstTimerNoID' :                    f'{year}AbsenteeRejFirstTimerNoID',
                'QC5nAbsenteeRejNoApplicationOnRecord' :             f'{year}AbsenteeRejNoApplicationOnRecord',
                'QE1aProvisionalSubmitted' :                         f'{year}TotalProvisionalSubmitted',
                'QE1bProvisionalCountedFullBallot' :                 f'{year}TotalProvisionalCountedFullBallot',
                'QE1cProvisionalCountedPartialBallot' :              f'{year}TotalProvisionalCountedPartialBallot',
                'QE1dProvisionalRejected' :                          f'{year}TotalProvisionalRejected',
                'QE2aProvisionalRejVoterNotRegistered' :             f'{year}ProvisionalRejVoterNotRegistered',
                'QE2bProvisionalRejWrongJurisdiction' :              f'{year}ProvisionalRejWrongJurisdiction',
                'QE2cProvisionalRejWrongPrecinct' :                  f'{year}ProvisionalRejWrongPrecinct',
                'QE2dProvisionalRejInsufficientID' :                 f'{year}ProvisionalRejInsufficientID',
                'QE2eProvisionalRejIncompleteOrIllegible' :          f'{year}ProvisionalRejIncompleteOrIllegible',
                'QE2fProvisionalRejBallotMissionFromEnvelope' :      f'{year}ProvisionalRejBallotMissionFromEnvelope',
                'QE2gProvisionalRejNoSignature' :                    f'{year}ProvisionalRejNoSignature',
                'QE2hProvisionalRejNonMatchingSig' :                 f'{year}ProvisionalRejNonMatchingSig',
                'QE2iProvisionalRejAlreadyVoted' :                   f'{year}ProvisionalRejAlreadyVoted',
                'QD1aNumPrecincts' :                                 f'{year}OperationsNumPrecincts',
                'QD2aNumPhysicalPlaces' :                            f'{year}OperationsNumPhysicalPlaces',
                'QD2bNumPhysicalOtherThanElectionOffices' :          f'{year}OperationsNumPhysicalOtherThanElectionOffices',
                'QD2fPhysicalEarlyVotingPlaces' :                    f'{year}OperationsPhysicalEarlyVotingPlaces',
                'QD3aNumPollWorkers' :                               f'{year}OperationsNumPollWorkers',
                'QD4aPWUnder18' :                                    f'{year}OperationsPWUnder18',
                'QD4bPW19_25' :                                      f'{year}OperationsPW19_25',
                'QD4cPW26_40' :                                      f'{year}OperationsPW26_40',
                'QD4dPW41_60' :                                      f'{year}OperationsPW41_60',
                'QD4ePW61_70' :                                      f'{year}OperationsPW61_70',
                'QD4fPW70Plus' :                                     f'{year}OperationsPW70Plus',
                'QD5PWRecruitingDifficulty' :                        f'{year}OperationsPWRecruitingDifficulty'
            }, axis=1)

        # There can be stray columns where
        # all values are NaN (their names are
        # like 'Unnamed : 47'
        df = df.fillna(0)
        
        # Remove Guam, Virgin Islands, AMERICAN SAMOA, Puerto Rico:
        df = df.drop(df[df['State'] == 'GU'].index)
        df = df.drop(df[df['State'] == 'VI'].index)
        df = df.drop(df[df['State'] == 'AS'].index)
        df = df.drop(df[df['State'] == 'PR'].index)

        # Create the 5-digit FIPS version, where the
        # first two digits designate the State, and
        # remaining 3 denote the county:

        countyFIPS = self.parse_fips_codes(df[[f'FIPSCodeDetailed', 
                                               'State',
                                               'Jurisdiction']],
                                              self.geocodes[year])


        df_new = df.merge(countyFIPS, on='FIPSCodeDetailed')

        # Move the county FIPS col to after the Jurisdiction:
        # Get all the cols other than {year}FIPSCounty, which
        # the merge put at the end:

        cols = list(df_new.columns.values)
        county_col_name = cols.pop()
        juris_pos = 1+cols.index('Jurisdiction')
        head = cols[:juris_pos]
        tail = cols[juris_pos:]
        new_cols = head + [county_col_name] + tail
        df = df_new[new_cols]
        
        # Rename the FIPS col to conform to our norms:
        df = df.rename({county_col_name : f'{year}CountyFIPS'}, axis=1)

        # Make multiindex:
        mindx_df = df[['FIPSCodeDetailed','State','Jurisdiction']].copy()
        mindx_df.columns = ['FIPSDetailed','State','Jurisdiction']

        mindx_df['Election'] = [year]*len(df)
        mindx = pd.MultiIndex.from_frame(mindx_df,
                                         names=['FIPSDetailed',
                                                'State',
                                                'Jurisdiction',
                                                'Election'
                                                ])
        df.index = mindx
        # Remove the now superfluous cols:
        df_final = df.drop(['FIPSCodeDetailed', 'State', 'Jurisdiction'], axis=1)

        return df_final

    #------------------------------------
    # add_swingstate_bool 
    #-------------------
    
    def add_swingstate_bool(self, df):
        '''
        Given a df with at least a State
        column, add an additional col at the end
        called "Swingstate", which is True or False.
        
        @param df: any df that has a State column,
            which is a State abbreviation
        @type df: pd.DataFrame
        @return a new df with Battleground column added
        @rtype: pd.DataFrame
        '''
        # Get rows that are in swing States:
        swings = df.query("State in ['AZ','CO','FL','GA','IA','ME','MI','NC','OH','PA','TX','WI']")
        # Get their FIPS:
        swing_fips = swings.index.get_level_values('FIPSDetailed')
        
        # Make a new Swingstate col with all False; same
        # number of rows as df, where this col will be
        # adjoined. The index will just be the FIPS codes:
        swinger_col = pd.Series([False]*len(df), 
                                name='Swingstate', 
                                index=df.index.get_level_values('FIPSDetailed'))
        
        # Set only the swing state rows to True:
        swinger_col[swing_fips] = True 
        df['Swingstate'] = swinger_col.values 
        
        return df

    #------------------------------------
    # compute_percentages_2018
    #-------------------
    
    def compute_percentages_2018(self, df, year):


        df_perc = self.percentages
        df_perc[f'{year}CountyFIPS'] = df[f'{year}CountyFIPS']
        
        # VOTE BY MAIL

        df_perc[f'{year}PercByMailTotal'] = \
            self.avg_juris(df[f'{year}TotalVoteByMail'], df[f'{year}TotalVoteCounted'])
        
        # Percentage rejected:
        df_perc[f'{year}PercByMailRejTotal'] = \
            self.avg_juris(df[f'{year}ByMailCountByMailRejected'], df[f'{year}ByMailCountBallotsReturned'])
            
        # Percentages of reasons why rejected:
        df_perc[f'{year}PercByMailRejDeadline'] = \
            self.avg_juris(df[f'{year}ByMailRejDeadline'], df[f'{year}ByMailCountByMailRejected'])

        df_perc[f'{year}PercByMailRejSignatureMissing'] = \
            self.avg_juris(df[f'{year}ByMailRejSignatureMissing'], df[f'{year}ByMailCountByMailRejected'])

        df_perc[f'{year}PercByMailRejWitnessSignature'] = \
            self.avg_juris(df[f'{year}ByMailRejWitnessSignature'], df[f'{year}ByMailCountByMailRejected'])

        df_perc[f'{year}PercByMailRejNonMatchingSig'] = \
            self.avg_juris(df[f'{year}ByMailRejNonMatchingSig'], df[f'{year}ByMailCountByMailRejected'])

        df_perc[f'{year}PercByMailRejNoElectionOfficialSig'] = \
            self.avg_juris(df[f'{year}ByMailRejNoElectionOfficialSig'], df[f'{year}ByMailCountByMailRejected'])

        df_perc[f'{year}PercByMailRejUnofficialEnvelope'] = \
            self.avg_juris(df[f'{year}ByMailRejUnofficialEnvelope'], df[f'{year}ByMailCountByMailRejected'])

        df_perc[f'{year}PercByMailRejBallotMissing'] = \
            self.avg_juris(df[f'{year}ByMailRejBallotMissing'], df[f'{year}ByMailCountByMailRejected'])

        df_perc[f'{year}PercByMailRejEnvelopeNotSealed'] = \
            self.avg_juris(df[f'{year}ByMailRejEnvelopeNotSealed'], df[f'{year}ByMailCountByMailRejected'])

        df_perc[f'{year}PercByMailRejNoAddr'] = \
            self.avg_juris(df[f'{year}ByMailRejNoAddr'], df[f'{year}ByMailCountByMailRejected'])

        df_perc[f'{year}PercByMailRejMultipleBallots'] = \
            self.avg_juris(df[f'{year}ByMailRejMultipleBallots'], df[f'{year}ByMailCountByMailRejected'])

        df_perc[f'{year}PercByMailRejDeceased'] = \
            self.avg_juris(df[f'{year}ByMailRejDeceased'], df[f'{year}ByMailCountByMailRejected'])

        df_perc[f'{year}PercByMailRejAlreadyVoted'] = \
            self.avg_juris(df[f'{year}ByMailRejAlreadyVoted'], df[f'{year}ByMailCountByMailRejected'])

        df_perc[f'{year}PercByMailRejNoVoterId'] = \
            self.avg_juris(df[f'{year}ByMailRejNoVoterId'], df[f'{year}ByMailCountByMailRejected'])

        df_perc[f'{year}PercByMailRejNoBallotApplication'] = \
            self.avg_juris(df[f'{year}ByMailRejNoBallotApplication'], df[f'{year}ByMailCountByMailRejected'])

        # Percentage of provisional ballots:
        
        df_perc[f'{year}PercProvisionalsRej'] = \
            self.avg_juris( df[f'{year}ProvisionalRejCountTotal'], df[f'{year}ProvisionalCountTotal'])

        df_perc[f'{year}PercByProvRejNotRegistered'] = \
            self.avg_juris(df[f'{year}ProvisionalRejProvisionalNotRegistered'], df[f'{year}ProvisionalCountRejected'])

        df_perc[f'{year}PercByProvRejWrongJurisdiction'] = \
            self.avg_juris(df[f'{year}ProvisionalRejWrongJurisdiction'], df[f'{year}ProvisionalCountRejected'])

        df_perc[f'{year}PercByProvRejWrongPrecinct'] = \
            self.avg_juris(df[f'{year}ProvisionalRejWrongPrecinct'], df[f'{year}ProvisionalCountRejected'])

        df_perc[f'{year}PercByProvRejNoID'] = \
            self.avg_juris(df[f'{year}ProvisionalRejNoID'], df[f'{year}ProvisionalCountRejected'])

        df_perc[f'{year}PercByProvRejIncomplete'] = \
            self.avg_juris(df[f'{year}ProvisionalRejIncomplete'], df[f'{year}ProvisionalCountRejected'])

        df_perc[f'{year}PercByProvRejBallotMissing'] = \
            self.avg_juris(df[f'{year}ProvisionalRejBallotMissing'], df[f'{year}ProvisionalCountRejected'])

        df_perc[f'{year}PercByProvRejNoSig'] = \
            self.avg_juris(df[f'{year}ProvisionalRejNoSig'], df[f'{year}ProvisionalCountRejected'])

        df_perc[f'{year}PercByProvRejSigNotMatching'] = \
            self.avg_juris(df[f'{year}ProvisionalRejSigNotMatching'], df[f'{year}ProvisionalCountRejected'])

        df_perc[f'{year}PercByProvRejAlreadyVoted'] = \
            self.avg_juris(df[f'{year}ProvisionalRejAlreadyVoted'], df[f'{year}ProvisionalCountRejected'])

        df_perc[f'{year}PercProvisionalsRej'] = \
            self.avg_juris( df[f'{year}ProvisionalCountRejected'], df[f'{year}ProvisionalCountTotal'])

        df_perc[f'{year}PercByProvRejNotRegistered'] = \
            self.avg_juris(df[f'{year}ProvisionalRejProvisionalNotRegistered'], df[f'{year}ProvisionalCountRejected'])

        df_perc[f'{year}PercByProvRejWrongJurisdiction'] = \
            self.avg_juris(df[f'{year}ProvisionalRejWrongJurisdiction'], df[f'{year}ProvisionalCountRejected'])

        df_perc[f'{year}PercByProvRejWrongPrecinct'] = \
            self.avg_juris(df[f'{year}ProvisionalRejWrongPrecinct'], df[f'{year}ProvisionalCountRejected'])

        df_perc[f'{year}PercByProvRejNoID'] = \
            self.avg_juris(df[f'{year}ProvisionalRejNoID'], df[f'{year}ProvisionalCountRejected'])

        df_perc[f'{year}PercByProvRejIncomplete'] = \
            self.avg_juris(df[f'{year}ProvisionalRejIncomplete'], df[f'{year}ProvisionalCountRejected'])

        df_perc[f'{year}PercByProvRejBallotMissing'] = \
            self.avg_juris(df[f'{year}ProvisionalRejBallotMissing'], df[f'{year}ProvisionalCountRejected'])

        df_perc[f'{year}PercByProvRejNoSig'] = \
            self.avg_juris(df[f'{year}ProvisionalRejNoSig'], df[f'{year}ProvisionalCountRejected'])

        df_perc[f'{year}PercByProvRejSigNotMatching'] = \
            self.avg_juris(df[f'{year}ProvisionalRejSigNotMatching'], df[f'{year}ProvisionalCountRejected'])

        df_perc[f'{year}PercByProvRejAlreadyVoted'] = \
            self.avg_juris(df[f'{year}ProvisionalRejAlreadyVoted'], df[f'{year}ProvisionalCountRejected'])
            
        # Percentages Voting Modality:

        df_perc[f'{year}PercVoteModusAbroad'] = \
            self.avg_juris(df[f'{year}TotalVotedAbroad'], df[f'{year}TotalVoteCounted'])

        df_perc[f'{year}PercVoteModusProvisionalBallot'] = \
            self.avg_juris(df[f'{year}TotalVoteProvisionalBallot'], df['2018TotalCountVotesCast'])

        df_perc[f'{year}PercVoteModusInPersonEarly'] = \
            self.avg_juris(df[f'{year}TotalVoteInPersonEarly'], df['2018TotalCountVotesCast'])

        df_perc[f'{year}PercVoteModusPhysically'] = \
            self.avg_juris(df[f'{year}TotalVotedPhysically'], df['2018TotalCountVotesCast'])

        return df_perc

    #------------------------------------
    # avg_juris
    #-------------------
    
    def avg_juris(self, part, whole):
        '''
        Returns percentage of part in the whole.
        Both args are pd.Series. Used for survey
        responses, where multiple reporting 
        jurisdictions can be contained within one
        County FIPS code. Example (note the identidcal
        state/county: 50 005; Vermont, County 005):
        
                                                          SomeResponse
            5000562200    VT     SAINT JOHNSBURY  2018        309
            5000564075    VT     SHEFFIELD        2018         18
            5000569925    VT     STANNARD         2018          5
            5000571575    VT     SUTTON           2018         29

        Method groups by State, Jurisdiction, and Election, and
        averages all responses by jurisdictions within the same
        county.
        
        When whole is 0, returns 0. Same if either
        part or whole are NaN.
        
        @param part: number for which percentage is
            to be computed.
        @type part: numeric
        @param whole: the number that is 100%
        @type whole: numeric
        @return: percentage, averaged if needed
        @rtype: pd.Series
        '''
        
        #res = 100 * part.groupby(['State', 'Jurisdiction', 'Election']).mean()/whole
        res = 100 * part/whole
        res = res.where(~res.isna(),0)
        res = res.where(res != np.inf,0)
        return res 

    #------------------------------------
    # join_surveys
    #-------------------
    
    def join_surveys(self, df_dict):
        
        dfs = list(df_dict.values())
        if len(dfs) == 1:
            return dfs[0]
        df_merged = dfs[0]
        for nxt_df in dfs[1:]:
            df_merged = df_merged.merge(nxt_df, 
                                        left_on=['FIPSDetailed','State','Jurisdiction'], 
                                        right_on=['FIPSDetailed','State','Jurisdiction'],
                                        how='outer')

        return df_merged

    #------------------------------------
    # parse_fips_codes 
    #-------------------
    
    def parse_fips_codes(self, codes_and_states, geocodes):
        '''
        Wisconsin identifies each survey row with a 3-to-5-digit 
        County Subdivision code instead of the 10-digit codes that everyone
        else uses. This method finds the respective county codes.
        It also fixes a handful of other problems with the survey
        FIPS entries.
        
        Given a column of FIPS codes from that identify each row in 
        the Election Administration Voting Survey,
        with a second column containing the two-letter State abbreviations, 
        and a third with County names, return a new column of Country FIPS
        codes where each FIPS code uses the 5-col format:
        ss cccc: state and country.  
        
        Assumptions for codes_and_states:
           o 10-or-less FIPS code column is named 'FIPSCodeDetailed'
           o State 2-letter abbrev is called 'State'
           o County name column is called 'Jurisdiction' 
        
        @param codes_and_states: information on States
            and Counties represented in the EAV survey.
        @type codes_and_states: pd.DataFrame
        @param geocodes: Census geocodes table
        @type geocodes: pd.DataFrame
        @return FIPS codes converted to 5-digit convention,
            if needed.
        @rtype pd.Series
        '''
        
        # Add StateFIPS to codes_and_States (6460 rows):
        
        code_state_fips = codes_and_states.merge(self.state_fips,
                               on='State',
                               how='left'
                               )
        
        # code_state_fips is now (6457 rows):
        #      FIPSCodeDetailed State       Jurisdiction StateFull StateFIPS
        # 0          0100100000    AL     AUTAUGA COUNTY   Alabama        01
        # 1          0100300000    AL     BALDWIN COUNTY   Alabama        01
        # 2          0100500000    AL     BARBOUR COUNTY   Alabama        01
        # 3          0100700000    AL        BIBB COUNTY   Alabama        01
        #                     ...

        # Add column County for the normal FIPS of
        # length > 5. First two digits is State FIPS,
        # Next three digits is County FIPS. For now,
        # put nan into the counties with the short FIPS
        # entries from the survey:
        code_state_fips['County'] = pd.Series(np.where(code_state_fips.FIPSCodeDetailed.str.len() > 5,
                                              code_state_fips.FIPSCodeDetailed.str[:5],
                                              np.nan))
        # code_state_fips is now (6457 rows):
        #      FIPSCodeDetailed State       Jurisdiction StateFull StateFIPS County
        # 0          0100100000    AL     AUTAUGA COUNTY   Alabama        01  01001
        # 1          0100300000    AL     BALDWIN COUNTY   Alabama        01  01003
        # 2          0100500000    AL     BARBOUR COUNTY   Alabama        01  01005
        #                         ...
        # 1316               23    ME                          MAINE - UOCAVA      Maine        23    NaN
        # 4584            00100    WI  CITY OF ABBOTSFORD - MULTIPLE COUNTIES  Wisconsin        55    NaN
        # 4585            00175    WI          TOWN OF ABRAMS - OCONTO COUNTY  Wisconsin        55    NaN
        # 4586            00225    WI        TOWN OF ACKLEY - LANGLADE COUNTY  Wisconsin        55    NaN


        # Pull out the irregular FIPSCodeDetailed rows:
        
        problems = \
           code_state_fips[code_state_fips.FIPSCodeDetailed.str.len() <=5][['FIPSCodeDetailed',
                                                                            'State',
                                                                            'Jurisdiction',
                                                                            'StateFIPS']]
           
        # problems is now (1851 rows):
        #      FIPSCodeDetailed State                            Jurisdiction
        # 1316               23    ME                          MAINE - UOCAVA
        # 4584            00100    WI  CITY OF ABBOTSFORD - MULTIPLE COUNTIES
        # 4585            00175    WI          TOWN OF ABRAMS - OCONTO COUNTY
        # 4586            00225    WI        TOWN OF ACKLEY - LANGLADE COUNTY

        # Standardize jurisdictions to lower case for 
        # more reliable join later:
        problems.Jurisdiction = problems.Jurisdiction.str.lower()
                
        # Some jurisdictions are of the form:
        #   city of abbotsford - multiple counties
        #   town of abrams - oconto county
        # Pull out the part without the county:
        
        problems.Jurisdiction = problems.Jurisdiction.str.split(' -').apply(lambda town_county: town_county[0])
        
        # Convert geocode County names to lower case to 
        # match what we did with the corresponding names
        # in the surveys:
        ref_stateFIPS_juris = pd.DataFrame(geocodes[['StateFIPS', 'County']])
        ref_stateFIPS_juris['Jurisdiction'] = geocodes.Jurisdiction.str.lower()
        
        # ref_stateFIPS_juris (43770 rows):
        #       StateFIPS County    Jurisdiction
        # 0            00    000   united states
        # 1            01    000         alabama
        # 2            01    001  autauga county
        #                    ...

        # In surveys towns are called 'town blueberry',
        # while in Census geocodes they are called like
        # 'blueberry town'. Switch the Jurisdiction names
        # in problems: 
        pat  = r"(?P<one>town of) (?P<two>.*)"
        repl = lambda m: m.group('two')+' town'
        problems.Jurisdiction = problems.Jurisdiction.str.replace(pat,repl)

        # Same with 'city raspberry' and 'raspberry city':
        pat  = r"(?P<one>city of) (?P<two>.*)"
        repl = lambda m: m.group('two')+' city'
        problems.Jurisdiction = problems.Jurisdiction.str.replace(pat,repl)
        
        # ... and village:
        pat  = r"(?P<one>village of) (?P<two>.*)"
        repl = lambda m: m.group('two')+' village'
        problems.Jurisdiction = problems.Jurisdiction.str.replace(pat,repl)
        
        # Some counties are spelled differently in 
        # the surveys than in the Census. Make the 
        # survey version match the Census names:
        repl_dict = {"fontana village"       : "fontana-on-geneva lake village",
                    "grand view town"        : "grandview town",
                    "land o-lakes town"      : "land o'lakes town",
                    "lavalle village"        : "la valle village",
                    "mt. sterling village"   : "mount sterling village",
                    "saint lawrence town"    : "st. lawrence town"}
        
        problems.Jurisdiction = problems.Jurisdiction.replace(repl_dict)
        
        # Combine all info via the Jurisdiction key:
        solution = problems.merge(ref_stateFIPS_juris, 
                                  on=['Jurisdiction','StateFIPS'], 
                                  how='left',
                                  suffixes=['', '_y'])
        # Add State FIPS in front of the County triplet:
        solution.County = solution.StateFIPS.str.cat(solution.County)

        # A very occasional county FIPS comes up NaN (one in 2018):
        solution = solution.dropna()
         
        # Enhance our emerging good table with the
        # now known County FIPS of the short entries:
        code_state_fips_all = code_state_fips.merge(solution,
                                                    on=['FIPSCodeDetailed', 'StateFIPS'],
                                                    how='left',
                                                    suffixes=['_orig','_fixed']
                                                    ).drop_duplicates(subset='FIPSCodeDetailed')
        
        # code_state_fips_all is now (note how Count_orig
        # and County_fixed 'take turns' being NaN; they 
        # complement each other:
        
        #       FIPSCodeDetailed State_orig  Jurisdiction_orig ... County_orig ... County_fixed
        # 0           0100100000         AL     AUTAUGA COUNTY       01001           NaN
        # 1           0100300000         AL     BALDWIN COUNTY ...   01003     ...   NaN
        # 2           0100500000         AL     BARBOUR COUNTY       01005           NaN
        # 3           0100700000         AL        BIBB COUNTY       01007           NaN
        # 4           0100900000         AL      BLOUNT COUNTY       01009           NaN
        #                                            ...
        # 
        # 1316                23         ME   MAINE - UOCAVA           NaN     ...   23000
        # 4584             00100         WI   CITY OF ABBOTSFORD -...  NaN           55019
        # 4587             00175         WI          TOWN OF ABRAMS... NaN           55083
        # 4588             00225         WI        TOWN OF ACKLEY -... NaN           55067
        # 4589             00275         WI            CITY OF ADAMS...NaN           27099

        
        
        # With County_orig having County FIPS codes that were
        # easy to derive from the 10 digit FIPS. And County_fixed
        # with the hard-won codes. Each of the two cols is nan where
        # the other has a valid value. 
        
        # Copy the 'County_fixed' to 'County_orig' where
        # the latter are NaN:

        good_counties = code_state_fips_all.County_orig.fillna(code_state_fips_all.County_fixed)
        code_state_fips_all['CountyFIPS'] = good_counties

        #                (6460):
        #                0       001    
        #                1       003
        #                2       005
        #                3       007
        
        # Make the County FIPS single column's
        # index match the survey index. The 
        # 'drop=True' prevents the current index
        # of code_state_fips_all to be made into a second
        # column:
        
        return code_state_fips_all[['FIPSCodeDetailed', 'CountyFIPS']].reset_index(drop=True)

    #------------------------------------
    # fit
    #-------------------

    def fit(self, _X):
        return self

    #------------------------------------
    # transform 
    #-------------------

    def transform(self, year):
        '''
        Returns the cleaned up election info
        df of the given year. The work was already
        done during instantiation.
        
        @param year: election year for which cleaned
            data is requested.
        @type year: int
        '''
        if year == 2014:
            df = self.clean_survey_2014(self.EAVS_FILES[2014])
        elif year == 2016:
            df = self.clean_survey_2016(self.EAVS_FILES[2016])
        elif  year == 2018:
            df = self.clean_survey_2018(self.EAVS_FILES[2018])
        else:
            raise NotImplementedError(f"No cleanup capability for year {year}")

        return df

# ------------------------ Main ------------
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     description="Clean Election Administration Voting Survey Excel files"
                                     )

    parser.add_argument('-l', '--errLogFile',
                        help='fully qualified log file name to which info and error messages \n' +\
                             'are directed. Default: stdout.',
                        dest='errLogFile',
                        default=None)
    parser.add_argument('years',
                        type=int,
                        nargs='+',
                        help='The election year or years whose survey to clean (repeatable)',
                        )
    parser.add_argument('outfile',
                        help='The new (Excel) file where to write the result')

    args = parser.parse_args();

    xformer = ElectionSurveyCleaner()
    dfs = {}
    #*********
    args.years = [2018]
    #*********
    for year in args.years:
        dfs[year] = xformer.transform(year)
        
    # Combine the dfs:
    xformer.log.info(f"Combining {len(dfs)} surveys into one...")
    df_all = xformer.join_surveys(dfs)
    xformer.log.info(f"Done combining {len(dfs)} surveys into one.")
    
    xformer.log.info(f"Adding a Swingstate column...")
    df_all = xformer.add_swingstate_bool(df_all)
    xformer.log.info(f"Done adding a Swingstate column")
    
    xformer.log.info(f"Writing result to {args.outfile}...")
    outpath   = Path(args.outfile)
    out_csv_details       = outpath.parent.joinpath(f'{outpath.stem}.csv')
    out_csv_percentages   = outpath.parent.joinpath(f'{outpath.stem}_percentages.csv')
    #out_excel = outpath.parent.joinpath(outpath.stem + '.xlsx')
    
    xformer.log.info(f"Writing detail results to {out_csv_details}...")
    df_all.to_csv(out_csv_details, header=True, index=True)
    xformer.log.info(f"Done writing detail results to {out_csv_details}.")
    
    xformer.log.info(f"Writing percentages results to {out_csv_percentages}...")
    xformer.percentages.to_csv(out_csv_percentages, header=True, index=True)
    xformer.log.info(f"Done writing percentages results to {out_csv_percentages}.")


