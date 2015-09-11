# -*- coding: utf-8 -*-
"""
Created on Mon Sep 07 15:26:54 2015

@author: bitxi_000
"""
import math

R = 6473 # the R of earth is <R> km

def geodist(coord_1, coord_2):
    """
    This function calculates the distance between two points 
    on the earth surface in terms of kilometers.
    Input:
        coord_1: (lon:float, lat:float)
        coord_2: (lon:float, lat:float)
    Returns:
        dist: int
    """
    lon1, lat1 = coord_1[0] * math.pi / 180, coord_1[1] * math.pi / 180
    lon2, lat2 = coord_2[0] * math.pi / 180, coord_2[1] * math.pi / 180
    
    dlon, dlat = lon2 - lon1, lat2 - lat1
    
    a = math.pow(math.sin(dlat/2),2) \
        + math.cos(lat1)*math.cos(lat2)*math.pow(math.sin(dlon/2),2)
    
    c = 2*math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c 
    
    
