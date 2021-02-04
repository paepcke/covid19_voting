'''
Created on Oct 20, 2020
@author: paepcke
Spreadsheets from the Election Administration Surveys have
non-intuitive column names. For each election, a dict below
maps those into human readable column names. The dicts are
each used for two purposes: to only read columns from the 
huge spreadsheet that we need, and to rename the dataframe
columns.
Dicts are ordered.
The overall spreadsheet_maps (at the end of the file) keys off 
election years, providing the mapping dict for each; e.g.:
        spreadsheet_maps[2018]  
'''
from _collections import OrderedDict

year = 2018
spreadsheet_map_2018 = OrderedDict(
    {
     'FIPSCode':          f'FIPSCodeDetailed',
     'Jurisdiction_Name': f'Jurisdiction',
     'State_Full':        f'{year}State_Full',
     'State_Abbr':        f'{year}State_Abbr',
     'C1a':               f'{year}ByMailCountBallotsSent',
     'C1b':               f'{year}ByMailCountBallotsReturned',
     'C1c':               f'{year}ByMailRejUndeliverable',
     'C1d':               f'{year}ByMailRejVoided',
     'C1e':               f'{year}ByMailRejVotedInPerson',
     'C2a':               f'{year}ByMailCountPermanentByMailTransmitted',
     'C3a':               f'{year}ByMailCountCounted',
     'C4a':               f'{year}ByMailCountByMailRejected',
     'C4b':               f'{year}ByMailRejDeadline',
     'C4c':               f'{year}ByMailRejSignatureMissing',
     'C4d':               f'{year}ByMailRejWitnessSignature',
     'C4e':               f'{year}ByMailRejNonMatchingSig',
     'C4f':               f'{year}ByMailRejNoElectionOfficialSig',
     'C4g':               f'{year}ByMailRejUnofficialEnvelope',
     'C4h':               f'{year}ByMailRejBallotMissing',
     'C4i':               f'{year}ByMailRejEnvelopeNotSealed',
     'C4j':               f'{year}ByMailRejNoAddr',
     'C4k':               f'{year}ByMailRejMultipleBallots',
     'C4l':               f'{year}ByMailRejDeceased',
     'C4m':               f'{year}ByMailRejAlreadyVoted',
     'C4n':               f'{year}ByMailRejNoVoterId',
     'C4o':               f'{year}ByMailRejNoBallotApplication',
     'C4p_Other':         f'{year}ByMailRejOtherReason1',
     'C4p':               f'{year}ByMailRejOtherReasonCount1',
     'C4q_Other':         f'{year}ByMailRejOtherReason2',
     'C4q':               f'{year}ByMailRejOtherReason2Count',
     'C4r_Other':         f'{year}ByMailRejOtherReason3',
     'C4r':               f'{year}ByMailRejOtherReason3Count',
     'D1a':               f'{year}TotalCountVotesCast',
     'D2a':               f'{year}TotalCountVotedAtPoll',
     'D2b':               f'{year}TotalCountVotedEarlyPhysical',
     'D2Comments':        f'{year}CommentsEarlyVotingPhysical',
     'D3a':               f'{year}OperationsNumOfPrecincts',
     'D4a':               f'{year}OperationsNumPollingPlacesElectDay',
     'D5a':               f'{year}OperationsNumEarlyVotingPlaces',
     'D6' :               f'{year}OperationsNumPollWorkersElectDay',
     'D7' :               f'{year}OperationsNumPollWorkersEarlyVoting',
     'D6_D7Comments':     f'{year}OperationsPollingStationComments',
     'D8a':               f'{year}OperationsNumPollWorkers',
     'D8b':               f'{year}OperationsPWUnder18',
     'D8c':               f'{year}OperationsPW18_25',
     'D8d':               f'{year}OperationsPW26_40',
     'D8e':               f'{year}OperationsPW41_60',
     'D8f':               f'{year}OperationsPW61_70',
     'D8g':               f'{year}OperationsPW71Plus',
     'D8Comments':        f'{year}OperationsPWComments',
     'D9':                f'{year}OperationsPWRecruitingDifficulties',
     'E1a':               f'{year}ProvisionalCountTotal',
     'E1b':               f'{year}ProvisionalCountCountedFully',
     'E1c':               f'{year}ProvisionalCountCountedPartially',
     'E1d':               f'{year}ProvisionalCountRejected',
     'E1e_Other':         f'{year}ProvisionalCountRejOther',
     'E1Comments':        f'{year}CommentsProvisional',
     'E2a':               f'{year}ProvisionalRejCountTotal',
     'E2b':               f'{year}ProvisionalRejProvisionalNotRegistered',
     'E2c':               f'{year}ProvisionalRejWrongJurisdiction',
     'E2d':               f'{year}ProvisionalRejWrongPrecinct',
     'E2e':               f'{year}ProvisionalRejNoID',
     'E2f':               f'{year}ProvisionalRejIncomplete',
     'E2g':               f'{year}ProvisionalRejBallotMissing',
     'E2h':               f'{year}ProvisionalRejNoSig',
     'E2i':               f'{year}ProvisionalRejSigNotMatching',
     'E2j':               f'{year}ProvisionalRejAlreadyVoted',
     'E2k_Other':         f'{year}ProvisionalRejOther1Txt',
     'E2k':               f'{year}ProvisionalRej1Count',
     'E2l_Other':         f'{year}ProvisionalRejOther2Txt',
     'E2l':               f'{year}ProvisionalRejOther2Count',
     'E2m_Other':         f'{year}ProvisionalRejOther3Txt',
     'E2Comments':        f'{year}RejProvisionalOther3Count',
     'F1a':               f'{year}TotalVoteCounted',
     'F1b':               f'{year}TotalVotedPhysically',
     'F1c':               f'{year}TotalVotedAbroad',
     'F1d':               f'{year}TotalVoteByMail',
     'F1e':               f'{year}TotalVoteProvisionalBallot',
     'F1f':               f'{year}TotalVoteInPersonEarly',
     'F1g':               f'{year}TotalVoteByMailOnlyJurisdiction',
     'F1h_Other':         f'{year}TotalVoteOtherTxt',
     'F1h':               f'{year}TotalVoteOtherCount'
     })

spreadsheet_maps = {
    2018 : spreadsheet_map_2018
    }
