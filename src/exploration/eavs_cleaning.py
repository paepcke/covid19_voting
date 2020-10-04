#!/usr/bin/env python3
'''
Created on Sep 30, 2020

@author: paepcke
'''
import argparse
import os
import sys

from sklearn.base import BaseEstimator, TransformerMixin

import pandas as pd
from utils.logging_service import LoggingService

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
    
    INT_CONVERSIONS = {2018 : INT_CONVERSIONS2018,
                       2016 : INT_CONVERSIONS2016,
                       }

    #------------------------------------
    # Constructor 
    #-------------------

    def __init__(self):
        '''
        Constructor
        '''
        self.log = LoggingService()
    
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
                'State_Full'                          : f'State_Full{year}',
                'State_Abbr'                          : f'State_Abbr',
                'C1aMailBallotsSent'                  : f'MailBallotsSent{year}',
                'C1bMailBallotsReturned'              : f'MailBallotsReturned{year}',
                'C1cUndeliverable'                    : f'Undeliverable{year}',
                'C1dVoided'                           : f'Voided{year}',
                'C1eVotedInPerson'                    : f'VotedInPerson{year}',
                'C2aPermanentByMailTransmitted'       : f'PermanentByMailTransmitted{year}',
                'C3aByMailCounted'                    : f'ByMailCounted{year}',
                'C4aByMailRejected'                   : f'ByMailRejected{year}',
                'C4bRejDeadline'                      : f'RejDeadline{year}',
                'C4cRejSignatureMissing'              : f'RejSignatureMissing{year}',
                'C4dRejWitnessSignature'              : f'RejWitnessSignature{year}',
                'C4eRejNonMatchingSig'                : f'RejNonMatchingSig{year}',
                'C4fRejNoElectionOfficialSig'         : f'RejNoElectionOfficialSig{year}',
                'C4gRejUnofficialEnvelope'            : f'RejUnofficialEnvelope{year}',
                'C4hRejBallotMissing'                 : f'RejBallotMissing{year}',
                'C4iRejEnvelopeNotSealed'             : f'RejEnvelopeNotSealed{year}',
                'C4jRejNoAddr'                        : f'RejNoAddr{year}',
                'C4kRejMultipleBallots'               : f'RejMultipleBallots{year}',
                'C4lRejDeceased'                      : f'RejDeceased{year}',
                'C4mRejAlreadyVoted'                  : f'RejAlreadyVoted{year}',
                'C4nRejNoVoterId'                     : f'RejNoVoterId{year}',
                'C4oRejNoBallotApplication'           : f'RejNoBallotApplication{year}',
                'C4pRejOther1ReasonTxt'               : f'RejOtherReason1{year}',
                'C4pRejOther1ReasonCount'             : f'RejOtherReasonCount1{year}',
                'C4qRejOther2ReasonTxt'               : f'RejOtherReason2{year}',
                'C4qRejOther2ReasonCount'             : f'RejOtherReason2Count{year}',
                'D1aVotesCast'                        : f'VotesCast{year}',
                'D2aVotedAtPoll'                      : f'VotedAtPoll{year}',
                'D2bVotedEarlyPhysical'               : f'VotedEarlyPhysical{year}',
                'D2CommentsEarlyVotingPhysical'       : f'CommentsEarlyVotingPhysical{year}',
                'D3aNumOfPrecincts'                   : f'NumOfPrecincts{year}',
                'D4aNumPollingPlacesElectDay'         : f'NumPollingPlacesElectDay{year}',
                'D5aNumEarlyVotingPlaces'             : f'NumEarlyVotingPlaces{year}',
                'D6NumPollWorkersElectDay'            : f'NumPollWorkersElectDay{year}',
                'D7NumPollWorkersEarlyVoting'         : f'NumPollWorkersEarlyVoting{year}',
                'D6_D7CommentsPollingStationComments' : f'PollingStationComments{year}',
                'D8aNumPollWorkers'                   : f'NumPollWorkers{year}',
                'D8bPWUnder18'                        : f'PWUnder18{year}',
                'D8cPW18_25'                          : f'PW18_25{year}',
                'D8d26_40'                            : f'26_40{year}',
                'D8ePW41_60'                          : f'PW41_60{year}',
                'D8fPW61_70'                          : f'PW61_70{year}',
                'D8gPW71Plus'                         : f'PW71Plus{year}',
                'D8CommentsPW'                        : f'PWComments{year}',
                'D9PWRecruitingDifficulties'          : f'PWRecruitingDifficulties{year}',
                'E1aProvisionalTotal'                 : f'ProvisionalTotal{year}',
                'E1bProvisionalCountedFully'          : f'ProvisionalCountedFully{year}',
                'E1cProvisionalCountedPartially'      : f'ProvisionalCountedPartially{year}',
                'E1dProvisionalRejected'              : f'ProvisionalRejected{year}',
                'E1CommentsProvisional'               : f'CommentsProvisional{year}',
                'E2aRejProvisionalTotal'              : f'RejProvisionalTotal{year}',
                'E2bRejProvisionalNotRegistered'      : f'RejProvisionalNotRegistered{year}',
                'E2cRejProvisionalWrongJurisdiction'  : f'RejProvisionalWrongJurisdiction{year}',
                'E2dRejProvisionalWrongPrecinct'      : f'RejProvisionalWrongPrecinct{year}',
                'E2eRejProvisionalNoID'               : f'RejProvisionalNoID{year}',
                'E2fRejProvisionalIncomplete'         : f'RejProvisionalIncomplete{year}',
                'E2gRejProvisionalBallotMissing'      : f'RejProvisionalBallotMissing{year}',
                'E2hRejProvisionalNoSig'              : f'RejProvisionalNoSig{year}',
                'E2iRejProvisionalSigNotMatching'     : f'RejProvisionalSigNotMatching{year}',
                'E2jRejAlreadyVoted'                  : f'RejAlreadyVoted{year}',
                'E2kRejProvisionalOther1Txt'          : f'RejProvisionalOther1{year}',
                'E2kRejProvisionalOther1Count'        : f'RejProvisional1Count{year}',
                'E2lOtherRejProvisionalOther2Txt'     : f'OtherRejProvisionalOther2{year}',
                'E2lRejProvisionalOther2Count'        : f'RejProvisionalOther2Count{year}',
                'E2mRejProvisionalOther3Txt'          : f'RejProvisionalOther3Txt{year}',
                'E2nRejProvisionalOther3Count'        : f'RejProvisionalOther3Count{year}',
                'F1aVoteCounted'                      : f'VoteCounted{year}',
                'F1bVotedPhysically'                  : f'VotedPhysically{year}',
                'F1cVotedAbroad'                      : f'VotedAbroad{year}',
                'F1dVoteByMail'                       : f'VoteByMail{year}',
                'F1eVoteProvisionalBallot'            : f'VoteProvisionalBallot{year}',
                'F1fVoteInPersonEarly'                : f'VoteInPersonEarly{year}',
                'F1gVoteByMailJurisdiction'           : f'VoteByMailJurisdiction{year}'
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
        
        # Same for uses of 'Does not apply' and 'Data not available'
        df = df.replace()

#         # Make 10-digit FIPSCode into str, b/c the leading
#         # zeroes are confused with Octal:
#         df = df.astype({f'FIPSCodeDetailed' : str})
        
        # The State abbreviation needs to be right after
        # the detailed FIPS code to be uniform with other
        # sheets:
        
        st_abbr = df[f'State_Abbr']
        df = df.drop(f'State_Abbr', axis=1)
        df.insert(1,f'State', st_abbr)

        # Create the 5-digit FIPS version, where the
        # first two digits designate the State, and
        # remaining 3 denote the county:
        
        df.insert(3, f'FIPSCoarse{year}', df[f'FIPSCodeDetailed'].str[:5])
        
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
                              dtype={'FIPSCode' : str},
                              converters=self.INT_CONVERSIONS[year]
                              )
        self.log.info(f"Done reading Election Administration and Voting Survey for {year}.")
        df = sheet.rename({'FIPSCode'                                 : f'FIPSDetailed',
                           'JurisdictionName'                         : f'Jurisdiction',
                           'F1aVoterTurnout'                          : f'VoteTotalCount{year}',
                           'F1bVoteAtPhysical'                        : f'VoteAtPhysicalCenter{year}',
                           'F1cVoteAbroad'                            : f'VoteAbroadCount{year}',
                           'F1dVoteAbsentee'                          : f'VoteAbsenteeCount{year}',
                           'F1eVoteProvisional'                       : f'VoteProvisionalCount{year}',
                           'F1fVoteEarlyPhysical'                     : f'VoteEarlyBallotCentersCount{year}',
                           'F1gVoteByMail'                            : f'VoteByMailCount{year}',
                           'F3VoteFirstTimersIDNeededAndProvided'     : f'VoteNeededAndHadIDCount{year}',
                           'E1aProvisionalTotal'                      : f'VoteProvisionalCount{year}',
                           'E1bProvisionalCountedFull'                : f'VoteProvCountedFull{year}',
                           'E1cProvisionalCountedPartially'           : f'VoteProvCountedPartial{year}',
                           'E1dProvisionalRejectedTotal'              : f'VoteProvRejectedCount{year}',
                           'E2aProvisionalRejVoterNotInState'         : f'VoteProvRejNotInState{year}',
                           'E2bProvisionalRejWrongJurisdiction'       : f'VoteProvRejWrongJurisdiction{year}',
                           'E2cProvisionalRejWrongPrecinct'           : f'VoteProvRejWrongPrecinct{year}',
                           'E2dProvisionalRejInsufficientID'          : f'VoteProvRejInsufficientId{year}',
                           'E2eProvisionalRejIncompleteOrIllegible'   : f'VoteProvRejIllegible{year}',
                           'E2fProvisionalRejBallotMissing'           : f'VoteProvRejBallotMissing{year}',
                           'E2gProvisionalRejNoSig'                   : f'VoteProvRejNoSig{year}',
                           'E2hProvisionalRejNonMatchingSig'          : f'VoteProvRejSigNotMatching{year}',
                           'E2iProvisionalRejAlreadyVoted'            : f'VoteProvRejAlreadyVoted{year}',
                           'E2j_OtherProvisionalRejOther'             : f'VoteProvRejOther{year}',
                           'C1aAbsenteeSent'                          : f'VoteAbsenteeReturnedForCounting{year}',
                           'C1bAbsenteeReturnedForCounting'           : f'VoteAbsenteeCount{year}',
                           'C1cVoteAbsenteeUndeliverable'             : f'VoteAbsenteeUndeliverable{year}',
                           'C1dAbsenteeSpoiled'                       : f'VoteAbsenteeSpoiled{year}',
                           'C2AbsenteePermanentList?'                 : f'VoteAbsenteePermanentListYesNo{year}',
                           'C3AbsenteeSentBecauseOnPermanentList'     : f'VoteAbsenteeVotedFromPermanentListCount{year}',
                           'C4aAbsenteeCounted'                       : f'VoteAbsenteeCount{year}',
                           'C4bAbsenteeRejected'                      : f'VoteAbsenteeRejectedCount{year}',
                           'C5aAbsenteeRejLate'                       : f'VoteAbsenteeRejLate{year}',
                           'C5bAbsenteeRejNoSig'                      : f'VoteAbsenteeRejNoSig{year}',
                           'C5cAbsenteeRejNoWitnessSig'               : f'VoteAbsenteeRejNoWitnessSig{year}',
                           'C5dAbsenteeRejSigNotMatching'             : f'VoteAbsenteeRejSigNotMatching{year}',
                           'C5eAbsenteeRejNoElectOfficialSig'         : f'VoteAbsenteeRejNoElectionOfficialSig{year}',
                           'C5fAbsenteeRejUnofficialEnvelope'         : f'VoteAbsenteeRejNonOfficialEnvelope{year}',
                           'C5gAbsenteeRejBallotMissing'              : f'VoteAbsenteeRejBallotMissing{year}',
                           'C5hAbsenteeRejEnvNotSealed'               : f'VoteAbsenteeRejEnvNotSealed{year}',
                           'C5iAbsenteeRejNoResidentAddr'             : f'VoteAbsenteeRejNoResidentAddr{year}',
                           'C5jAbsenteeRejMultipleBallots'            : f'VoteAbsenteeRejMultipleBallotsInEnv{year}',
                           'C5kAbsenteeRejVoterDeceased'              : f'VoteAbsenteeRejVoterDeceased{year}',
                           'C5lAbsenteeRejAlreadyVoted'               : f'VoteAbsenteeRejAlreadyVoted{year}',
                           'C5mAbsenteeRejFirstTimeVoterBadID'        : f'VoteAbsenteeRejBadId{year}',
                           'C5nAbsenteeRejNoApplication'              : f'VoteAbsenteeRejNoApplication{year}',
                           'D1aNumPrecincts'                          : f'NumPrecincts{year}',
                           'D1CommentsPrecincts'                      : f'NumPrecinctsComments{year}',
                           'D2aNumPollingPlaces'                      : f'NumPollingPlaces{year}',
                           'D3aNumPollingWorkers'                     : f'NumPollWorkers{year}',
                           'D4aPWUnder18'                             : f'PWUnder18{year}',
                           'D4bPW18_25'                               : f'PW18_25{year}',
                           'D4c26_40'                                 : f'PW26_40{year}',
                           'D4dPW41_60'                               : f'PW41_60{year}',
                           'D4ePW61_70'                               : f'PW61_70{year}',
                           'D4fPWOver70'                              : f'PW71Plus{year}',
                           'D5PWRecruitingDifficulty'                 : f'PWRecruitingDifficulty{year}',
                           'D5CommentsPW'                             : f'PWDifficultyComments'
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
                        
        df = df.replace(to_replace=-888888, value=0)
        df = df.replace(to_replace=-999999, value=0)
        
        # Make 10-digit FIPSCode into str, b/c the leading
        # zeroes are confused with Octal:
        df = df.astype({f'FIPSDetailed' : str})
        
        # Create the 5-digit FIPS version, where the
        # first two digits designate the State, and
        # remaining 3 denote the county:
        
        df.insert(3, f'FIPSCoarse{year}', df[f'FIPSDetailed'].str[:5])
        
        # Make multiindex:
        mindx_df = pd.DataFrame(
            df[['FIPSDetailed','State','Jurisdiction']],
               columns=['FIPSDetailed','State','Jurisdiction']
               )
        mindx_df['Election'] = [year]*len(df)
        mindx = pd.MultiIndex.from_frame(mindx_df,
                                         names=['FIPSDetailed',
                                                'State',
                                                'Jurisdiction',
                                                'Election'
                                                ])
        df.index = mindx

        return df

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
                        default=None);
    parser.add_argument('year',
                        type=int,
                        help='The election year whose survey to clean',
                        action='store_true');
    parser.add_argument('outfile',
                        help='The new (Excel) file where to write the result')

    args = parser.parse_args();

    df = ElectionSurveyCleaner().transform(args.year)
    df.to_excel(args.outfile)
