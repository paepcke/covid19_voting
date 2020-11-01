#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 22 19:23:37 2020

@author: sidu
"""

alphaOut = alpha
rOut = r
pointIdxOut = pointIdx
newsplit = true

while newsplit:
    newsplit = False
    total_lines = len(rOut)
    for k in range(total_lines-1):
        a1 = pointIdx[k,0]
        b2 = pointIdx[k+1,1]
        alpha, r = FitLine(theta[a1:b2], rho[a1:b2])
        s = FindSplit(theta[a1:b2], rho[a1:b2], alpha, r, params)
        
        