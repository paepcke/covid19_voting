'''
Created on Sep 21, 2020

@author: paepcke
'''
import csv
import os


class CovidUtils(object):
    '''
    This is a singleton.
    '''
    __instance = None
    __is_initialized = False
    
    #-------------------------
    # __new__ 
    #--------------
    
    def __new__(cls):
        if CovidUtils.__instance is None:
            CovidUtils.__instance = object.__new__(cls)
        return CovidUtils.__instance
    
    #-------------------------
    # __repr__ 
    #--------------
    
    def __repr__(self):
        return f'<CovidUtils {hex(id(self))}>'
        
    #-------------------------
    # Constructor 
    #--------------
    
    def __init__(self):
        '''
        Constructor
        '''
        if CovidUtils.__is_initialized:
            return
        else:
            # Directory with various data files:
            self.data_dir = os.path.join(os.path.dirname(__file__), '../../data')
            self.import_state_mappings()

            CovidUtils.__is_initialized = True

    #------------------------------------
    # state_abbrev_series 
    #-------------------

    @classmethod
    def state_abbrev_series(cls, state_name_series):
        '''
        Takes an pandas Series of full State names, and
        returns a pd Series of the corresponding State
        abbreviations. 
        
        @param state_name_series: the full-name States
            to resolve to 2-letter abbreviations
        @type state_name_series: pd.Series
        @returns a new Series with abbreviations that
            correspond to the given full State names
            (done pairwise).
        @rtype pd.Series
        '''
        # One 'state' name in the voter turnout Excel
        # sheets is 'United States', leave that abbrev
        # NaN to conform with the newer turnout sheets:
        
        res = state_name_series.apply(lambda state_nm: 'US' \
                                      if state_nm == 'United States' 
                                      else cls.state_abbrevs[state_nm])
        return res

    #------------------------------------
    # import_state_mappings
    #-------------------
    
    def import_state_mappings(self):
        '''
        Initializes States related lookup dicts.
        The created mapping is for the numeric State
        numbers used in the Google Trends query count
        data. These mappings are not for connecting
        two-letter State abbrevs and the full State
        names.
        
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
          {'New York' :   'NY',
               ...}
               
        4. self.abbrevs_state: 2-letter abbreviations to long State name
           Abbrev   :  LongName
        {  'NY'   :   'New York,
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

        CovidUtils.state_abbrevs = {}
        with open(os.path.join(self.data_dir, 'state_abbrevs.csv'), 'r', encoding='utf-8-sig') as fd:
            reader = csv.reader(fd)
            for (longName, abbrev) in reader:
                CovidUtils.state_abbrevs[longName] = abbrev
                
        CovidUtils.abbrevs_state = {abbrev : state_name for 
                                    (abbrev, state_name) in zip(self.state_abbrevs.values(),
                                                                self.state_abbrevs.keys())}
