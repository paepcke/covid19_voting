'''
Created on Sep 22, 2020

@author: paepcke
'''

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

class Visualizer(object):
    '''
    classdocs
    '''

    #------------------------------------
    # Constructor 
    #-------------------

    def __init__(self):
        '''
        Constructor
        '''
        pass
    
    #------------------------------------
    # feature_importance 
    #-------------------
    
    def feature_importance(self, forest, feature_names):

        importances = forest.feature_importances_
        std = np.std([tree.feature_importances_ for tree in forest.estimators_],
                     axis=0)
        importance_indices = np.argsort(importances)[::-1]
        
        plt.tight_layout()
        fig, ax = plt.subplots(figsize=(9,6))

        num_bars = len(importances)
        ax.bar(range(num_bars),
               importances[importance_indices],
               color="r", yerr=std[importance_indices], align="center")
        
        fig.suptitle('Feature Importances')
        ax.set_xticks(range(num_bars))
        ax.set_xticklabels(feature_names[importance_indices], rotation=45, ha='right')
        ax.set_y('Mean Decrease in Variance Across Trees')
        plt.show()
        
    #------------------------------------
    # correlation_matrix 
    #-------------------
    
    def correlation_matrix(self, X, y):

        all_matrix = pd.concat([y,X],axis=1)
        plt.tight_layout()
        _fig, ax = plt.subplots(figsize=(9,6))
        cor = all_matrix.corr()
        heatmap = sns.heatmap(cor, annot=True, cmap=plt.cm.Reds, ax=ax)
        heatmap.set_xticklabels(heatmap.get_xticklabels(), rotation=45)
        plt.show() 

    #------------------------------------
    # real_and_estimated
    #-------------------
    
    def real_and_estimated(self, xlabels, predicted, truth):
        '''
        Creates line chart with two lines: true
        voter turnout and predicted 
        voter turnout.
        
        @param forest: fully fit random forest
        @type forest: RandomForestRegressor
        @param X_test: feature matrix to use for prediction
        @type X_test: pd.DataFrame
        @param y_test: true voter turnout for each election
        @type y_test: pd.Series
        '''
        
        # Get the prediction turnouts:
        
        fig = plt.figure()
        ax  = fig.add_subplot(1,1,1) # chart at grid coords 1,1, axes # 1
        ax.plot(xlabels,predicted)
        #display(fig)
        print('foo')
        
        

