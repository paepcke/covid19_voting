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
from prediction.covid_utils import CovidUtils
from utils.logging_service import LoggingService


sys.path.append(os.path.join(os.path.dirname(__file__), '.'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))



def catch_non_int(value):
    try:
        return int(value)
    except ValueError:
        return 0


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
                                               '../../data/Exploration/inAbsentia2018.xlsx')
                            }
    INT_CONVERSIONS2018 = {
        'FIPSCode' : str,
        'Jurisdiction_Name' : str,
        'State_Full' : str,
        'State_Abbr' : str,
        'C1aMailBallotsSent' : catch_non_int,
        'C1bMailBallotsReturned' : catch_non_int,
        'C1cUndeliverable' : catch_non_int,
        'C1dVoided' : catch_non_int,
        'C1eVotedInPerson' : catch_non_int,
        'C2aPermanentByMailTransmitted' : catch_non_int,
        'C3aByMailCounted' : catch_non_int,
        'C4aByMailRejected' : catch_non_int,
        'C4bRejDeadline' : catch_non_int,
        'C4cRejSignatureMissing' : catch_non_int,
        'C4dRejWitnessSignature' : catch_non_int,
        'C4eRejNonMatchingSig' : catch_non_int,
        'C4fRejNoElectionOfficialSig' : catch_non_int,
        'C4gRejUnofficialEnvelope' : catch_non_int,
        'C4hRejBallotMissing' : catch_non_int,
        'C4iRejEnvelopeNotSealed' : catch_non_int,
        'C4jRejNoAddr' : catch_non_int,
        'C4kRejMultipleBallots' : catch_non_int,
        'C4lRejDeceased' : catch_non_int,
        'C4mRejAlreadyVoted' : catch_non_int,
        'C4nRejNoVoterId' : catch_non_int,
        'C4oRejNoBallotApplication' : catch_non_int,
        'C4pRejOther1ReasonTxt' : str,
        'C4pRecOther1ReasonCount' : catch_non_int,
        'C4qRejOther2ReasonTxt' : str,
        'C4qRejOther2ReasonCount' : catch_non_int,
        'D1aVotesCast' : catch_non_int,
        'D2aVotedAtPoll' : catch_non_int,
        'D2bVotedEarlyPhysical' : catch_non_int,
        'D2CommentsEarlyVotingPhysical' : catch_non_int,
        'D3aNumOfPrecincts' : catch_non_int,
        'D4aNumPollingPlacesElectDay' : catch_non_int,
        'D5aNumEarlyVotingPlaces' : catch_non_int,
        'D6NumPollWorkersElectDay' : catch_non_int,
        'D7NumPollWorkersEarlyVoting' : catch_non_int,
        'D6_D7CommentsPollingStationComments' : str,
        'D8aNumPollWorkers' : catch_non_int,
        'D8bPWUnder18' : catch_non_int,
        'D8cPW18_25' : catch_non_int,
        'D8d26_40' : catch_non_int,
        'D8ePW41_60' : catch_non_int,
        'D8fPW61_70' : catch_non_int,
        'D8gPW71Plus' : catch_non_int,
        'D8CommentsPW' : catch_non_int,
        'D9PWRecruitingDifficulties' : catch_non_int,
        'E1aProvisionalTotal' : catch_non_int,
        'E1bProvisionalCountedFully' : catch_non_int,
        'E1cProvisionalCountedPartially' : catch_non_int,
        'E1dProvisionalRejected' : catch_non_int,
        'E1CommentsProvisional' : catch_non_int,
        'E2aRejProvisionalTotal' : catch_non_int,
        'E2bRejProvisionalNotRegistered' : catch_non_int,
        'E2cRejProvisionalWrongJurisdiction' : catch_non_int,
        'E2dRejProvisionalWrongPrecinct' : catch_non_int,
        'E2eRejProvisionalNoID' : catch_non_int,
        'E2fRejProvisionalIncomplete' : catch_non_int,
        'E2gRejProvisionalBallotMissing' : catch_non_int,
        'E2hRejProvisionalNoSig' : catch_non_int,
        'E2iRejProvisionalSigNotMatching' : catch_non_int,
        'E2jRejAlreadyVoted' : catch_non_int,
        'E2kRejProvisionalOther1Txt' : str,
        'E2kRejProvisionalOther1Count' : catch_non_int,
        'E2lRejProvisionalOther2Txt' : str,
        'E2lRejProvisionalOther2Count' : catch_non_int,
        'E2mRejProvisionalOther3Txt' : str,
        'E2nRejProvisionalOther3Count' : catch_non_int,
        'F1aVoteCounted' : catch_non_int,
        'F1bVotedPhysically' : catch_non_int,
        'F1cVotedAbroad' : catch_non_int,
        'F1dVoteByMail' : catch_non_int,
        'F1eVoteProvisionalBallot' : catch_non_int,
        'F1fVoteInPersonEarly' : catch_non_int,
        'F1gVoteByMailJurisdiction' : catch_non_int
    }
 
    INT_CONVERSIONS2016 = {
        'F1aVoterTurnout' : catch_non_int,
        'F1bVoteAtPhysical' : catch_non_int,
        'F1cVoteAbroad' : catch_non_int,
        'F1dVoteAbsentee' : catch_non_int,
        'F1eVoteProvisional' : catch_non_int,
        'F1fVoteEarlyPhysical' : catch_non_int,
        'F1gVoteByMail' : catch_non_int,
        'F3VoteFirstTimersIDNeededAndProvided' : catch_non_int,
        'E1aProvisionalTotal' : catch_non_int,
        'E1bProvisionalCountedFull' : catch_non_int,
        'E1cProvisionalCountedPartially' : catch_non_int,
        'E1dProvisionalRejectedTotal' : catch_non_int,
        'E2aProvisionalRejVoterNotInState' : catch_non_int,
        'E2bProvisionalRejWrongJurisdiction' : catch_non_int,
        'E2cProvisionalRejWrongPrecinct' : catch_non_int,
        'E2dProvisionalRejInsufficientID' : catch_non_int,
        'E2eProvisionalRejIncompleteOrIllegible' : catch_non_int,
        'E2fProvisionalRejBallotMissing' : catch_non_int,
        'E2gProvisionalRejNoSig' : catch_non_int,
        'E2hProvisionalRejNonMatchingSig' : catch_non_int,
        'E2iProvisionalRejAlreadyVoted' : catch_non_int,
        'E2j_OtherProvisionalRejOther' : catch_non_int,
        'C1aAbsenteeSent' : catch_non_int,
        'C1bAbsenteeReturnedForCounting' : catch_non_int,
        'C1cAbsenteeReturnedUndeliverable' : catch_non_int,
        'C1dAbsenteeSpoiled' : catch_non_int,
        'C2AbsenteePermanentList?' : catch_non_int,
        'C3AbsenteeSentBecauseOnPermanentList' : catch_non_int,
        'C4aAbsenteeCounted' : catch_non_int,
        'C4bAbsenteeRejected' : catch_non_int,
        'C5aAbsenteeRejLate' : catch_non_int,
        'C5bAbsenteeRejNoSig' : catch_non_int,
        'C5cAbsenteeRejNoWitnessSig' : catch_non_int,
        'C5dAbsenteeRejSigNotMatching' : catch_non_int,
        'C5eAbsenteeRejNoElectOfficialSig' : catch_non_int,
        'C5fAbsenteeRejUnofficialEnvelope' : catch_non_int,
        'C5gAbsenteeRejBallotMissing' : catch_non_int,
        'C5hAbsenteeRejEnvNotSealed' : catch_non_int,
        'C5iAbsenteeRejNoResidentAddr' : catch_non_int,
        'C5jAbsenteeRejMultipleBallots' : catch_non_int,
        'C5kAbsenteeRejVoterDeceased' : catch_non_int,
        'C5lAbsenteeRejAlreadyVoted' : catch_non_int,
        'C5mAbsenteeRejFirstTimeVoterBadID' : catch_non_int,
        'C5nAbsenteeRejNoApplication' : catch_non_int,
        'D1aNumPrecincts' : catch_non_int,
        'D2aNumPollingPlaces' : catch_non_int,
        'D3aNumPollingWorkers' : catch_non_int,
        'D4aPWUnder18' : catch_non_int,
        'D4bPW18_25' : catch_non_int,
        'D4c26_40' : catch_non_int,
        'D4dPW41_60' : catch_non_int,
        'D4ePW61_70' : catch_non_int,
        'D4fPWOver70' : catch_non_int,
        'D5PWRecruitingDifficulty' : catch_non_int
        }

    INT_CONVERSIONS2014 = {
        'FIPSCode' : str,
        'PreferredOrder' : str,
        'State' : str,
        'Jurisdiction' : str,
        'QF1aVoteTotalCount' : catch_non_int,
        'QF1bVoteNumInPhysicalLoc' : catch_non_int,
        'QF1cVoteAbroad' : catch_non_int,
        'QF1dVoteAbsentee' : catch_non_int,
        'QF1eVoteProvisional' : catch_non_int,
        'QF1fVoteAtEarlyVoteCenter' : catch_non_int,
        'QF1gVoteByMail' : catch_non_int,
        'QF3VoteProvidedID' : catch_non_int,
        'QF3_CommentVote' : catch_non_int,
        'QC1aNumAbsenteeBallotsTransmitted' : catch_non_int,
        'QC1bAbsenteeSentInForCounting' : catch_non_int,
        'QC1dAbsenteeSpoiled' : catch_non_int,
        'QC1eAbsenteeNotReturnedByVoter' : catch_non_int,
        'QC2AbsenteeHavePermanentListAutoSend' : catch_non_int,
        'QC3AbsenteeNumSentFromPermanentList' : catch_non_int,
        'QC4aAbsenteeTotalCounted' : catch_non_int,
        'QC4bAbsenteeNumRejected' : catch_non_int,
        'QC5aAbsenteeRejDeadline' : catch_non_int,
        'QC5bAbsenteeRejNoVoterSig' : catch_non_int,
        'QC5cAbsenteeRejNoWitnessSig' : catch_non_int,
        'QC5dAbsenteeRejNonMatchingSig' : catch_non_int,
        'QC5eAbsenteeRejNoElectionOfficialSig' : catch_non_int,
        'QC5fAbsenteeRejUnofficialEnvelope' : catch_non_int,
        'QC5gAbsenteeRejBallotMissing' : catch_non_int,
        'QC5hAbsenteeRejEnvelopeNotSealed' : catch_non_int,
        'QC5iAbsenteeRejNoResidentAddr' : catch_non_int,
        'QC5jAbsenteeRejMultipleBallotsInEnvelope' : catch_non_int,
        'QC5kAbsenteeRejVoterDeceased' : catch_non_int,
        'QC5lAbsenteeRejAlreadyVoted' : catch_non_int,
        'QC5mAbsenteeRejFirstTimerNoID' : catch_non_int,
        'QC5nAbsenteeRejNoApplicationOnRecord' : catch_non_int,
        'QE1aProvisionalSubmitted' : catch_non_int,
        'QE1bProvisionalCountedFullBallot' : catch_non_int,
        'QE1cProvisionalCountedPartialBallot' : catch_non_int,
        'QE1dProvisionalRejected' : catch_non_int,
        'QE2aProvisionalRejVoterNotRegistered' : catch_non_int,
        'QE2bProvisionalRejWrongJurisdiction' : catch_non_int,
        'QE2cProvisionalRejWrongPrecinct' : catch_non_int,
        'QE2dProvisionalRejInsufficientID' : catch_non_int,
        'QE2eProvisionalRejIncompleteOrIllegible' : catch_non_int,
        'QE2fProvisionalRejBallotMissionFromEnvelope' : catch_non_int,
        'QE2gProvisionalRejNoSignature' : catch_non_int,
        'QE2hProvisionalRejNonMatchingSig' : catch_non_int,
        'QE2iProvisionalRejAlreadyVoted' : catch_non_int,
        'QD1aNumPrecincts' : catch_non_int,
        'QD2aNumPhysicalPlaces' : catch_non_int,
        'QD2bNumPhysicalOtherThanElectionOffices' : catch_non_int,
        'QD2fPhysicalEarlyVotingPlaces' : catch_non_int,
        'QD3aNumPollWorkers' : catch_non_int,
        'QD4aPWUnder18' : catch_non_int,
        'QD4bPW19_25' : catch_non_int,
        'QD4cPW26_40' : catch_non_int,
        'QD4dPW41_60' : catch_non_int,
        'QD4ePW61_70' : catch_non_int,
        'QD4fPW70Plus' : catch_non_int,
        'QD5PWRecruitingDifficulty' : catch_non_int
        }


    INT_CONVERSIONS = {2018 : INT_CONVERSIONS2018,
                       2016 : INT_CONVERSIONS2016,
                       2014 : INT_CONVERSIONS2014
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
        # to percentages for each election. Will be:
        #    {2018 : {'{year}PercByMailRejTotal'    : <num>,
        #             '{year}PercByMailRej<reason>' : <num>,
        #                         ...
        #             '{year}PercHowVoted<modality>': <num>,
        #                         ...
        #             '{year}PercProvRej            : <num>,
        #             '{year}PercProvRej<reason>    : <num>,
        #                         ...
        #    {2016 : {...
        #    }
        # At the end we'll flatten into a CSV:
        
        self.percentages = {2018 : {},
                            2016 : {},
                            2014 : {}
                            }
        
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
           
        for year in {2014,2016, 2018}
        '''

        self.geocodes = {}
        for year in [2014, 2016, 2018]:
            # Get reference to the Census geo codes:
            geocode_file = os.path.join(os.path.dirname(__file__),
                                        f'../../data/Exploration/all-geocodes-v{year}.xlsx')
    
            self.log.info("Reading Census geo codes...")
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

        # Fixes for 2016:
        # ---------------
        
        new_entry = pd.DataFrame({'StateFIPS'    : '55',
                                  'County'       : '139',
                                  'Subdivision'  : '26982',
                                  'Jurisdiction' : 'Winnebago County'
                                  }, index=[len(self.geocodes[year])])
        self.geocodes[2016] = pd.concat([self.geocodes[2016], new_entry], axis=0)

        # Fix missing Maine abroad entry:
        new_entry = pd.DataFrame({'StateFIPS'   : '23.',
                                  'County'      : '000',
                                  'Subdivision' : '23',
                                  'Jurisdiction': 'MAINE - UOCAVA'
                                  }, index=[len(self.geocodes)])

        self.geocodes[2016] = pd.concat([self.geocodes[2016], new_entry], axis=0)

    #------------------------------------
    # clean_survey_2018 
    #-------------------
    
    def clean_survey_2018(self, survey_file):
        
        year = 2018
        self.log.info(f"Reading Election Administration and Voting Survey for {year}...")
        sheet = pd.read_excel(io=survey_file,
                              header=[0],
                              dtype={'FIPSCode' : str},
                              converters=self.INT_CONVERSIONS[year]
                              )        
        self.log.info(f"Done reading Election Administration and Voting Survey for {year}.")
        # Sometimes a col 'PreferredOrder sneaks in:
        try:
            sheet = sheet.drop('PreferredOrder', axis=1)
        except KeyError:
            pass


        df = sheet.rename({
                'FIPSCode'                            : f'FIPSCodeDetailed',
                'Jurisdiction_Name'                   : f'Jurisdiction',
                'State_Full'                          : f'{year}State_Full',
                'State_Abbr'                          : f'{year}State_Abbr',
                'C1aMailBallotsSent'                  : f'{year}ByMailCountBallotsSent',
                'C1bMailBallotsReturned'              : f'{year}ByMailCountBallotsReturned',
                'C1cUndeliverable'                    : f'{year}ByMailRejUndeliverable',
                'C1dVoided'                           : f'{year}ByMailRejVoided',
                'C1eVotedInPerson'                    : f'{year}ByMailRejVotedInPerson',
                'C2aPermanentByMailTransmitted'       : f'{year}ByMailCountPermanentByMailTransmitted',
                'C3aByMailCounted'                    : f'{year}ByMailCountCounted',
                'C4aByMailRejected'                   : f'{year}ByMailCountByMailRejected',
                'C4bRejDeadline'                      : f'{year}ByMailRejDeadline',
                'C4cRejSignatureMissing'              : f'{year}ByMailRejSignatureMissing',
                'C4dRejWitnessSignature'              : f'{year}ByMailRejWitnessSignature',
                'C4eRejNonMatchingSig'                : f'{year}ByMailRejNonMatchingSig',
                'C4fRejNoElectionOfficialSig'         : f'{year}ByMailRejNoElectionOfficialSig',
                'C4gRejUnofficialEnvelope'            : f'{year}ByMailRejUnofficialEnvelope',
                'C4hRejBallotMissing'                 : f'{year}ByMailRejBallotMissing',
                'C4iRejEnvelopeNotSealed'             : f'{year}ByMailRejEnvelopeNotSealed',
                'C4jRejNoAddr'                        : f'{year}ByMailRejNoAddr',
                'C4kRejMultipleBallots'               : f'{year}ByMailRejMultipleBallots',
                'C4lRejDeceased'                      : f'{year}ByMailRejDeceased',
                'C4mRejAlreadyVoted'                  : f'{year}ByMailRejAlreadyVoted',
                'C4nRejNoVoterId'                     : f'{year}ByMailRejNoVoterId',
                'C4oRejNoBallotApplication'           : f'{year}ByMailRejNoBallotApplication',
                'C4pRejOther1ReasonTxt'               : f'{year}ByMailRejOtherReason1',
                'C4pRejOther1ReasonCount'             : f'{year}ByMailRejOtherReasonCount1',
                'C4qRejOther2ReasonTxt'               : f'{year}ByMailRejOtherReason2',
                'C4qRejOther2ReasonCount'             : f'{year}ByMailRejOtherReason2Count',
                'D1aVotesCast'                        : f'{year}TotalCountVotesCast',
                'D2aVotedAtPoll'                      : f'{year}TotalCountVotedAtPoll',
                'D2bVotedEarlyPhysical'               : f'{year}TotalCountVotedEarlyPhysical',
                'D2CommentsEarlyVotingPhysical'       : f'{year}CommentsEarlyVotingPhysical',
                'D3aNumOfPrecincts'                   : f'{year}OperationsNumOfPrecincts',
                'D4aNumPollingPlacesElectDay'         : f'{year}OperationsNumPollingPlacesElectDay',
                'D5aNumEarlyVotingPlaces'             : f'{year}OperationsNumEarlyVotingPlaces',
                'D6NumPollWorkersElectDay'            : f'{year}OperationsNumPollWorkersElectDay',
                'D7NumPollWorkersEarlyVoting'         : f'{year}OperationsNumPollWorkersEarlyVoting',
                'D6_D7CommentsPollingStationComments' : f'{year}OperationsPollingStationComments',
                'D8aNumPollWorkers'                   : f'{year}OperationsNumPollWorkers',
                'D8bPWUnder18'                        : f'{year}OperationsPWUnder18',
                'D8cPW18_25'                          : f'{year}OperationsPW18_25',
                'D8d26_40'                            : f'{year}OperationsPW26_40',
                'D8ePW41_60'                          : f'{year}OperationsPW41_60',
                'D8fPW61_70'                          : f'{year}OperationsPW61_70',
                'D8gPW71Plus'                         : f'{year}OperationsPW71Plus',
                'D8CommentsPW'                        : f'{year}OperationsPWComments',
                'D9PWRecruitingDifficulties'          : f'{year}OperationsPWRecruitingDifficulties',
                'E1aProvisionalTotal'                 : f'{year}ProvisionalCountTotal',
                'E1bProvisionalCountedFully'          : f'{year}ProvisionalCountCountedFully',
                'E1cProvisionalCountedPartially'      : f'{year}ProvisionalCountCountedPartially',
                'E1dProvisionalRejected'              : f'{year}ProvisionalCountRejected',
                'E1CommentsProvisional'               : f'{year}CommentsProvisional',
                'E2aRejProvisionalTotal'              : f'{year}ProvisionalRejCountTotal',
                'E2bRejProvisionalNotRegistered'      : f'{year}ProvisionalRejProvisionalNotRegistered',
                'E2cRejProvisionalWrongJurisdiction'  : f'{year}ProvisionalRejWrongJurisdiction',
                'E2dRejProvisionalWrongPrecinct'      : f'{year}ProvisionalRejWrongPrecinct',
                'E2eRejProvisionalNoID'               : f'{year}ProvisionalRejNoID',
                'E2fRejProvisionalIncomplete'         : f'{year}ProvisionalRejIncomplete',
                'E2gRejProvisionalBallotMissing'      : f'{year}ProvisionalRejBallotMissing',
                'E2hRejProvisionalNoSig'              : f'{year}ProvisionalRejNoSig',
                'E2iRejProvisionalSigNotMatching'     : f'{year}ProvisionalRejSigNotMatching',
                'E2jRejAlreadyVoted'                  : f'{year}ProvisionalRejAlreadyVoted',
                'E2kRejProvisionalOther1Txt'          : f'{year}ProvisionalRejOther1Txt',
                'E2kRejProvisionalOther1Count'        : f'{year}ProvisionalRej1Count',
                'E2lOtherRejProvisionalOther2Txt'     : f'{year}ProvisionalRejOther2Txt',
                'E2lRejProvisionalOther2Count'        : f'{year}ProvisionalRejOther2Count',
                'E2mRejProvisionalOther3Txt'          : f'{year}ProvisionalRejOther3Txt',
                'E2nRejProvisionalOther3Count'        : f'{year}RejProvisionalOther3Count',
                'F1aVoteCounted'                      : f'{year}TotalVoteCounted',
                'F1bVotedPhysically'                  : f'{year}TotalVotedPhysically',
                'F1cVotedAbroad'                      : f'{year}TotalVotedAbroad',
                'F1dVoteByMail'                       : f'{year}TotalVoteByMail',
                'F1eVoteProvisionalBallot'            : f'{year}TotalVoteProvisionalBallot',
                'F1fVoteInPersonEarly'                : f'{year}TotalVoteInPersonEarly',
                'F1gVoteByMailJurisdiction'           : f'{year}TotalVoteByMailOnlyJurisdiction',
            }, axis=1)
            
        # There can be stray columns where
        # all values are NaN (their names are
        # like 'Unnamed : 47'
        df = df.loc[:,~(df.isna().all())]
        
        # Remove 'not applicables' and 'not availables' that
        # are expressed as negative integers:
        df = df.replace(to_replace=-888888, value=0)
        df = df.replace(to_replace=-999999, value=0)
        
        df = df.replace(to_replace={-888888 : 0,
                                    -999999 : 0,
                                    -88     : 0,
                                    'Does not apply' : 0,
                                    'Data not available' : 0
                                    })

#         # Make 10-digit FIPSCode into str, b/c the leading
#         # zeroes are confused with Octal:
#         df = df.astype({f'FIPSCodeDetailed' : str})
        
        # The State abbreviation needs to be right after
        # the detailed FIPS code to be uniform with other
        # sheets:
        
        st_abbr = df[f'{year}State_Abbr']
        df = df.drop(f'{year}State_Abbr', axis=1)
        df.insert(1,f'State', st_abbr)

        # Create the 5-digit FIPS version, where the
        # first two digits designate the State, and
        # remaining 3 denote the county:

        county_FIPS = self.parse_fips_codes(df[[f'FIPSCodeDetailed', 'State']],
                                               self.geocodes[year])
        df.insert(3, f'FIPSCounty{year}', county_FIPS)

        # Remove Guam, Virgin Islands, AMERICAN SAMOA, Puerto Rico:
        df = df.drop(df[df['State'] == 'GU'].index)
        df = df.drop(df[df['State'] == 'VI'].index)
        df = df.drop(df[df['State'] == 'AS'].index)
        df = df.drop(df[df['State'] == 'PR'].index)

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
        
        # Fill in the percentage calculations in
        # self.percentages for 2018:
        self.compute_percentages_2018(df_final, year)
        
        return df_final
    
    #------------------------------------
    # compute_percentages_2018
    #-------------------
    
    def compute_percentages_2018(self, df, year):

        # Percentage rejected:
        self.percentages[year][f'{year}PercByMailRejTotal'] = \
            100 * df[f'{year}ByMailCountByMailRejected'] / df[f'{year}ByMailCountBallotsReturned']
            
        # Percentages of reasons why rejected:
        self.percentages[year][f'{year}PercByMailRejDeadline'] = \
            100 * df[f'{year}ByMailRejDeadline'] / df[f'{year}ByMailCountByMailRejected']

        self.percentages[year][f'{year}PercByMailRejSignatureMissing'] = \
            100 * df[f'{year}ByMailRejSignatureMissing'] / df[f'{year}ByMailCountByMailRejected']

        self.percentages[year][f'{year}PercByMailRejWitnessSignature'] = \
            100 * df[f'{year}ByMailRejWitnessSignature'] / df[f'{year}ByMailCountByMailRejected']

        self.percentages[year][f'{year}PercByMailRejNonMatchingSig'] = \
            100 * df[f'{year}ByMailRejNonMatchingSig'] / df[f'{year}ByMailCountByMailRejected']

        self.percentages[year][f'{year}PercByMailRejNoElectionOfficialSig'] = \
            100 * df[f'{year}ByMailRejNoElectionOfficialSig'] / df[f'{year}ByMailCountByMailRejected']

        self.percentages[year][f'{year}PercByMailRejUnofficialEnvelope'] = \
            100 * df[f'{year}ByMailRejUnofficialEnvelope'] / df[f'{year}ByMailCountByMailRejected']

        self.percentages[year][f'{year}PercByMailRejBallotMissing'] = \
            100 * df[f'{year}ByMailRejBallotMissing'] / df[f'{year}ByMailCountByMailRejected']

        self.percentages[year][f'{year}PercByMailRejEnvelopeNotSealed'] = \
            100 * df[f'{year}ByMailRejEnvelopeNotSealed'] / df[f'{year}ByMailCountByMailRejected']

        self.percentages[year][f'{year}PercByMailRejNoAddr'] = \
            100 * df[f'{year}ByMailRejNoAddr'] / df[f'{year}ByMailCountByMailRejected']

        self.percentages[year][f'{year}PercByMailRejMultipleBallots'] = \
            100 * df[f'{year}ByMailRejMultipleBallots'] / df[f'{year}ByMailCountByMailRejected']

        self.percentages[year][f'{year}PercByMailRejDeceased'] = \
            100 * df[f'{year}ByMailRejDeceased'] / df[f'{year}ByMailCountByMailRejected']

        self.percentages[year][f'{year}PercByMailRejAlreadyVoted'] = \
            100 * df[f'{year}ByMailRejAlreadyVoted'] / df[f'{year}ByMailCountByMailRejected']

        self.percentages[year][f'{year}PercByMailRejNoVoterId'] = \
            100 * df[f'{year}ByMailRejNoVoterId'] / df[f'{year}ByMailCountByMailRejected']

        self.percentages[year][f'{year}PercByMailRejNoBallotApplication'] = \
            100 * df[f'{year}ByMailRejNoBallotApplication'] / df[f'{year}ByMailCountByMailRejected']

        # Percentage of provisional ballots:
        
        self.percentages[year][f'{year}PercProvisionalsRej'] = \
            100 *  df[f'{year}ProvisionalRejCountTotal'] / df[f'{year}ProvisionalCountTotal']

        self.percentages[year][f'{year}PercByProvRejNotRegistered'] = \
            100 * df[f'{year}ProvisionalRejProvisionalNotRegistered'] / df[f'{year}ProvisionalCountRejected']

        self.percentages[year][f'{year}PercByProvRejWrongJurisdiction'] = \
            100 * df[f'{year}ProvisionalRejWrongJurisdiction'] / df[f'{year}ProvisionalCountRejected']

        self.percentages[year][f'{year}PercByProvRejWrongPrecinct'] = \
            100 * df[f'{year}ProvisionalRejWrongPrecinct'] / df[f'{year}ProvisionalCountRejected']

        self.percentages[year][f'{year}PercByProvRejNoID'] = \
            100 * df[f'{year}ProvisionalRejNoID'] / df[f'{year}ProvisionalCountRejected']

        self.percentages[year][f'{year}PercByProvRejIncomplete'] = \
            100 * df[f'{year}ProvisionalRejIncomplete'] / df[f'{year}ProvisionalCountRejected']

        self.percentages[year][f'{year}PercByProvRejBallotMissing'] = \
            100 * df[f'{year}ProvisionalRejBallotMissing'] / df[f'{year}ProvisionalCountRejected']

        self.percentages[year][f'{year}PercByProvRejNoSig'] = \
            100 * df[f'{year}ProvisionalRejNoSig'] / df[f'{year}ProvisionalCountRejected']

        self.percentages[year][f'{year}PercByProvRejSigNotMatching'] = \
            100 * df[f'{year}ProvisionalRejSigNotMatching'] / df[f'{year}ProvisionalCountRejected']

        self.percentages[year][f'{year}PercByProvRejAlreadyVoted'] = \
            100 * df[f'{year}ProvisionalRejAlreadyVoted'] / df[f'{year}ProvisionalCountRejected']

        self.percentages[year][f'{year}PercProvisionalsRej'] = \
            100 *  df[f'{year}ProvisionalCountRejected'] / df[f'{year}ProvisionalCountTotal']

        self.percentages[year][f'{year}PercByProvRejNotRegistered'] = \
            100 * df[f'{year}ProvisionalRejProvisionalNotRegistered'] / df[f'{year}ProvisionalCountRejected']

        self.percentages[year][f'{year}PercByProvRejWrongJurisdiction'] = \
            100 * df[f'{year}ProvisionalRejWrongJurisdiction'] / df[f'{year}ProvisionalCountRejected']

        self.percentages[year][f'{year}PercByProvRejWrongPrecinct'] = \
            100 * df[f'{year}ProvisionalRejWrongPrecinct'] / df[f'{year}ProvisionalCountRejected']

        self.percentages[year][f'{year}PercByProvRejNoID'] = \
            100 * df[f'{year}ProvisionalRejNoID'] / df[f'{year}ProvisionalCountRejected']

        self.percentages[year][f'{year}PercByProvRejIncomplete'] = \
            100 * df[f'{year}ProvisionalRejIncomplete'] / df[f'{year}ProvisionalCountRejected']

        self.percentages[year][f'{year}PercByProvRejBallotMissing'] = \
            100 * df[f'{year}ProvisionalRejBallotMissing'] / df[f'{year}ProvisionalCountRejected']

        self.percentages[year][f'{year}PercByProvRejNoSig'] = \
            100 * df[f'{year}ProvisionalRejNoSig'] / df[f'{year}ProvisionalCountRejected']

        self.percentages[year][f'{year}PercByProvRejSigNotMatching'] = \
            100 * df[f'{year}ProvisionalRejSigNotMatching'] / df[f'{year}ProvisionalCountRejected']

        self.percentages[year][f'{year}PercByProvRejAlreadyVoted'] = \
            100 * df[f'{year}ProvisionalRejAlreadyVoted'] / df[f'{year}ProvisionalCountRejected']
            
        # Percentages Voting Modality:

        self.percentages[year][f'{year}PercVoteModusAbroad'] = \
            100 * df[f'{year}TotalVotedAbroad'] / df[f'{year}TotalVoteCounted']

        self.percentages[year][f'{year}PercVoteModusByMail'] = \
            100 * df[f'{year}TotalVoteByMail'] / df[f'{year}TotalVoteCounted']

        self.percentages[year][f'{year}PercVoteModusProvisionalBallot'] = \
            100 * df[f'{year}TotalVoteProvisionalBallot'] / df[f'{year}TotalVoteCounted']

        self.percentages[year][f'{year}PercVoteModusInPersonEarly'] = \
            100 * df[f'{year}TotalVoteInPersonEarly'] / df[f'{year}TotalVoteCounted']

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
                              dtype={'FIPSCode' : str},
                              converters=self.INT_CONVERSIONS[year]
                              )
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
        
        # Create the 5-digit FIPS version, where the
        # first two digits designate the State, and
        # remaining 3 denote the county:

        county_FIPS = self.parse_fips_codes(df[[f'FIPSCodeDetailed', 'State']],
                                            self.geocodes[year])
        
        df.insert(3, f'FIPSCounty{year}', county_FIPS)
        
        # Remove Guam, Virgin Islands, AMERICAN SAMOA, Puerto Rico:
        df = df.drop(df[df['State'] == 'GU'].index)
        df = df.drop(df[df['State'] == 'VI'].index)
        df = df.drop(df[df['State'] == 'AS'].index)
        df = df.drop(df[df['State'] == 'PR'].index)

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

    #------------------------------------
    # clean_survey_2014
    #-------------------
    
    def clean_survey_2014(self, survey_file):
        
        year = 2014
        self.log.info(f"Reading Election Administration and Voting Survey for {year}...")
        sheet = pd.read_excel(io=survey_file,
                              header=[0],
                              converters=self.INT_CONVERSIONS[year]
                              )        
        self.log.info(f"Done reading Election Administration and Voting Survey for {year}.")
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
        df = df.loc[:,~(df.isna().all())]
        
        # Remove 'not applicables' and 'not availables' that
        # are expressed as negative integers:
        
        df = df.replace(to_replace={-888888 : 0,
                                    -999999 : 0,
                                    -88     : 0,
                                    'Does not apply' : 0,
                                    'Data not available' : 0
                                    })

        # Create the 5-digit FIPS version, where the
        # first two digits designate the State, and
        # remaining 3 denote the county:

        county_FIPS = self.parse_fips_codes(df[[f'FIPSCodeDetailed', 'State']],
                                            self.geocodes[year])
        
        df.insert(3, f'FIPSCounty{year}', county_FIPS)

        # Remove Guam, Virgin Islands, AMERICAN SAMOA, Puerto Rico:
        df = df.drop(df[df['State'] == 'GU'].index)
        df = df.drop(df[df['State'] == 'VI'].index)
        df = df.drop(df[df['State'] == 'AS'].index)
        df = df.drop(df[df['State'] == 'PR'].index)

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
        
        Given a column of FIPS codes from that identify each row in 
        the Election Administration Voting Survey,
        with a second column containing the two-letter
        State abbreviation, return a new column of Country FIPS
        codes where each FIPS code using the 5-col format:
        ss cccc: state and country. 
        
        Assumptions:
           o FIPS code column is named 'FIPSCodeDetailed'
           o State 2-letter abbrev is called 'State' 
        
        @param codes_and_states: FIPS codes to parse,
            and their corresponding 2-letter State abbreviations
        @type codes_and_states: pd.DataFrame
        @param geocodes: Census geocodes table
        @type geocodes: pd.DataFrame
        @return FIPS codes converted to 5-digit convention,
            if needed.
        @rtype pd.Series
        '''
        
        # For convenience:
        fips_codes = codes_and_states['FIPSCodeDetailed']

        # Find FIPS code entries in current survey that
        # has only 5 or fewer digits: 
        prob_fips = fips_codes[fips_codes.map(len) <= 5].astype(str)
        # If all FIPS codes are 10 digits, we know
        # how to get State and County FIPS from that:
        # Grab digits 2,3,4 (0-based)
        if len(prob_fips) == 0:
            return fips_codes.str[2:5]
        
        prob_fips.name = 'Subdivision'
        
        # Combine problem FIPS with the official
        # 2018 table of FIPS codes, using Subdivision
        # as the join key:
        
        probs_info = geocodes.merge(prob_fips, on='Subdivision', how='right')
        
        num_bad_rows = probs_info.StateFIPS.isna().sum()
        self.log.info(f"Number of jurisdictions in survey but not in official FIPS list: {num_bad_rows}")

        # Find all the FIPSCodeDetailed rows where that
        # code is less than the usual 10 digits, and find
        # their proper counties:
        # 
        # WE NOW HAVE:
        #        self.state_fips
        #                      StateFull StateFIPS State
        #        0                Alabama         1    AL
        #        1                 Alaska         2    AK
        # 
        #        codes_and_states (6460 rows)
        #             FIPSCodeDetailed State
        #        0          0100100000    AL
        #        1          0100300000    AL
        # 
        # 
        #         probs_info (3715 rows)
        #              StateFIPS County Subdivision      Jurisdiction
        #         0           23    000          23    MAINE - UOCAVA
        #         1           23    021       00100        Abbot town
        #         2           27    111       00100   Aastad township
        
        # Add StateFIPS to codes_and_States (6460 rows):
        
        code_state_fips = codes_and_states.merge(self.state_fips,
                               on='State',
                               how='left'
                               )
        # 
        #               FIPSCodeDetailed State StateFull StateFIPS
        # 
        #         0          0100100000    AL   Alabama         01
        #         1          0100300000    AL   Alabama         01
        #         2          0100500000    AL   Alabama         01
        #                                 ...
        #         1318               23    ME      Maine        23
        #         4587            00100    WI  Wisconsin        55
        #         4588            00175    WI  Wisconsin        55
        #         4589            00225    WI  Wisconsin        55
        #                                         
        
        # Add column County for the normal FIPS of
        # length > 5. First two digits is State FIPS,
        # Nexgt three digits is County FIPS. For now,
        # put nan into the counties with the short FIPS
        # entries from the survey:
        code_state_fips['County'] = pd.Series(np.where(code_state_fips.FIPSCodeDetailed.str.len() > 5,
                                              code_state_fips.FIPSCodeDetailed.str[2:5],
                                              np.nan))
        
        #        code_state_fips (6040 rows)
        #             FIPSCodeDetailed State StateFull StateFIPS County
        #        0          0100100000    AL   Alabama         1    001
        #        1          0100300000    AL   Alabama         1    003
        #                                      ...
        #        1318               23    ME      Maine        23    NaN
        #        4587            00100    WI  Wisconsin        55    NaN
        #        4588            00175    WI  Wisconsin        55    NaN
        #        4589            00225    WI  Wisconsin        55    NaN

        # Pull out the rows where the County FIPS is 
        # now nan:
        code_state_fips_shorties = code_state_fips[code_state_fips.FIPSCodeDetailed.str.len() <=5]
        
        #        FIPSCodeDetailed State  StateFull StateFIPS County (1851)
        #        1318               23    ME      Maine        23    NaN
        #        4587            00100    WI  Wisconsin        55    NaN
        #        4588            00175    WI  Wisconsin        55    NaN
        
        # The short codes are subdivision FIPS. 
        # Grab the proper County FIPS from our info:
        code_fips_goodies = code_state_fips_shorties.merge(probs_info,
                                       left_on='FIPSCodeDetailed',
                                       right_on='Subdivision',
                                       suffixes=['_Bad','_Good']
                                       )[['FIPSCodeDetailed', 'StateFIPS_Good', 'County_Good', 'Jurisdiction']]
        
        #                   FIPSCodeDetailed StateFIPS_Good County_Good (3715 total, but 1851 unique FIPSCodeDetailed)
        #              0                  23             23         000
        #              1               00100             23         021
        #              2               00100             27         111
        #              3               00100             34         001
        #              4               00100             38         077
        # 
        #               code_fips_goodies[['FIPSCodeDetailed', 'StateFIPS_Good']].nunique(axis=0)
        #                  FIPSCodeDetailed    1851
        #                  StateFIPS_Good        20
        #                  dtype: int64
        

        # Enhance our emerging good table with the
        # now known County FIPS of the short entries:
        code_state_fips_all = code_state_fips.merge(code_fips_goodies,
                                                    on='FIPSCodeDetailed',
                                                    how='left'
                                                    ).drop_duplicates(subset='FIPSCodeDetailed')
        
        #                    (6460)
        #                    FIPSCodeDetailed State StateFull StateFIPS County StateFIPS_Good County_Good
        #               0          0100100000    AL   Alabama         1    001            NaN         NaN
        #               1          0100300000    AL   Alabama         1    003            NaN         NaN
        #               2          0100500000    AL   Alabama         1    005            NaN         NaN
        #                                                 ...
        #               1318               23    ME      Maine        23    NaN             23         000
        #               4587            00100    WI  Wisconsin        55    NaN             23         021
        #               4594            00175    WI  Wisconsin        55    NaN             20         153
        #               4596            00225    WI  Wisconsin        55    NaN             55         067
        #               4597            00275    WI  Wisconsin        55    NaN             20         131
        # 
        # Copy the 'good FIPS' to the County FIPS where the
        # latter are NaN. Also prepend the respective State FIPS
        # to create the 5-digit state-county FIPS code:
        good_counties = code_state_fips_all.StateFIPS.str.cat(code_state_fips_all.County.fillna(code_state_fips_all.County_Good))
        
        #                (6460):
        #                0       001    
        #                1       003
        #                2       005
        #                3       007
        
        # Make the County FIPS single column's
        # index match the survey index. The 
        # 'drop=True' prevents the current index
        # of good_counties to be made into a second
        # column:
        return good_counties.reset_index(drop=True)

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
        
        
        self.log.info("Creating CSV-ready percentages ...")
        header = ['year']
        rows   = []
        
        for year in self.percentages.keys():
            row = [year]
            header.append(self.percentages[year].keys())
            row.append(self.percentages[year].values())
            rows.append(row)
            
        self.percentages_csv = [header, rows]
        
        self.log.info("Done creating CSV-ready percentages .")

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
    out_csv   = outpath.parent.joinpath(outpath.stem + '.csv')
    out_excel = outpath.parent.joinpath(outpath.stem + '.xlsx')
    
    xformer.log.info(f"Writing result to {out_csv} as CSV...")
    df_all.to_csv(out_csv)
    xformer.log.info(f"Done writing result to {out_csv} as CSV.")
    
#     xformer.log.info(f"Writing result to {out_excel} as Excel...")
#     df_all.to_excel(out_excel)
#     xformer.log.info(f"Done writing result to {out_excel} as Excel.")
