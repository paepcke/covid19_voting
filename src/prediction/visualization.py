'''
Created on Sep 22, 2020

@author: paepcke
'''

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import matplotlib.patches as mpatches
from utils.logging_service import LoggingService

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
        self.log = LoggingService()
    
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
        fig.tight_layout()
        ax  = fig.add_subplot(1,1,1) # chart at grid coords 1,1, axes # 1
        
        self.log.info("Creating scatter plot of overall errors...")
        ax.scatter(truth, predicted)
        self.log.info("Done creating scatter plot of overall errors.")
        
        ax.set_xlabel('True Voter Turnout')
        ax.set_ylabel('Predicted Voter Turnout')
        
        # Now a grid of model performance by State:

        election_colors = pd.Series(['blue', 'green', 'red', 'cyan', 'magenta', 'black'],
                                    index=[2008, 2010, 2012, 2014, 2016, 2018],
                                    name='Colors')
        # Make color patches for the election year legend:
        # Start by making: 
        #  [('blue', 2008), ('green', 2010), ('red', 2012), ...]
        patch_info = zip(election_colors.values, election_colors.index)
        legend_patches = [mpatches.Patch(color=col, label=election) 
                          for (col,election) in patch_info]

        fig1, ax_plots = plt.subplots(13,4,sharex=True, sharey=True)
        
        #***fig1.tight_layout()
        fig1.set_size_inches(10,20)
        
        states = pd.Series([idx_entry[0] for idx_entry in truth.index]).unique()
        state_grid = states.reshape(13,4)
        
        self.log.info("Creating 13x4 scatter plots for model performance per State and election...")
        for row in range(13):
            for col in range(4):
                curr_ax = ax_plots[row,col]
                state = state_grid[row,col]
                state_truths_all_elections = truth.xs(state, level='Region')
                state_preds_all_elections  = predicted.xs(state, level='Region')
                # Put a dot for one election after the other
                # down in a different color:
                for x,y,c in zip(state_truths_all_elections,
                                 state_preds_all_elections, 
                                 election_colors):
                    ax_plots[row,col].scatter(x, y, color=c)
                    
                # Add the all-correct line:
                curr_ax.plot(state_truths_all_elections, 
                             state_truths_all_elections)
                
                # Add the State abbrev in the lower right:
                _txt_obj = curr_ax.text(0.8, 
                                        0.1,
                                        state,
                                        transform=curr_ax.transAxes
                                        )

        # Put x-axis label underneath the 2nd axes 
        # of the last line:
        ax_plots[12,1].set_xlabel('True Voter Turnout', fontsize='xx-large')
        
        # Put Y axis label relative to the axes [6,0] in the 
        # subplot grid so as to be roughly centered:
        ax_plots[6,0].set_ylabel('Predictioned Voter Turnout', fontsize='xx-large')

        # Place a 2-column legend just above the 
        # 2nd plot. Units are same as the units
        # within the axes. The 'expand' says to 
        # take as much horizontal space as needed:
        ax_plots[0,1].legend(handles=legend_patches,
                             title='Election',
                             mode='expand',
                             ncol=2,
                             loc=(-0.1,1.1)      # (x,y in units of the axes): Upper left corner
                             )
        
        # Add explanation for the lines in each subplot:
        ax_plots[0,1].text(0.8, 
                           1.0, 
                           "Line: perfect prediction", 
                           fontsize='large'
                           )
        # Figure title:
        ax_plots[0,1].text(0.8, 1.4, "Voter Turnout Prediction by State", fontsize='x-large', fontweight='bold')

        self.log.info("Done creating 13x4 scatter plots for model performance per State and election.")
        self.log.info("Drawing scatter plots...")
        plt.show()
        self.log.info("Done drawing scatter plots.")
        print('foo')
        
        

