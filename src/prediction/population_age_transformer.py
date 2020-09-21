'''
Created on Sep 21, 2020

@author: paepcke
'''

import pandas as pd
from sklearn import BaseEstimator, TransformerMixin

class PopulationAgeTransformer(BaseEstimator, TransformerMixin):
    '''
    classdocs
    '''


    def __init__(self, params):
        '''
        Constructor
        '''
        