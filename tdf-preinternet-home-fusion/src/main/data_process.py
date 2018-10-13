# -*- coding: utf-8 -*-
"""
Created on Mon Jul 16 08:17:59 2018

@author: tued7001
"""
import numpy as np
import pandas as pd
from numba.decorators import jit
from sklearn.preprocessing import LabelEncoder


@jit(nopython = True)
def jitHomeRW( brand_ids, cc_values, num_brand_ids, max_cc, weights ):
    """
    :param brand_ids:
    :param cc_values:
    :param num_brand_ids:
    :param max_cc:
    :param weights:
    :return:
    """
    n = weights.shape[0]
    
    brand_count = np.zeros((num_brand_ids,))
    cc_count = np.zeros((max_cc,))
    
    rws = np.zeros((n,))

    rw = 0.0

    for i in range(n):
        brand = brand_ids[i]
        cc = cc_values[i]
        
        if ( brand_count[brand] == 0.0) | ( cc_count[cc] == 0.0 ):
            rw = weights
            brand_count[brand] += 1
            cc_count[cc] += 1
        else:
            rw += weights[i]
            
        rws[i] = rw
        
    return rws

def getHomeUsage( nol_usage ):
    """
    :param nol_usage:
    :return:
    """
    twh_usage = nol_usage.loc[ nol_usage['duration'] > 0, :].groupby(['brand_cs_id', 'cc'])['weight'].sum().reset_index().rename( columns = {'weight' : 'twh'})
    
    brand_enc = LabelEncoder()
    brand_inds = brand_enc.fit_transform(nol_usage['brand_cs_id'].values)
    
    num_brand_ids =int(  nol_usage['brand_cs_id'].nunique() )
    max_cc = int( nol_usage['cc'].max() )
    
    #normalizes cc labels
    cc_inds = nol_usage['cc'].values - 1
    
    #use jit function
    nol_usage['rw'] = jitHomeRW( brand_inds, cc_inds, num_brand_ids, max_cc, nol_usage['weight'].values )
    
    #get the twh
    nol_usage = pd.merge( nol_usage[['brand_cs_id', 'cc', 'rw', 'duration']], 
                               twh_usage, 
                               on = ['brand_cs_id', 'cc'],
                               how = 'left')
    
    #scale our ratios
    nol_usage['rw_twh'] = 3.0*nol_usage['rw'] / nol_usage['twh']
    
    nol_usage['rw_twh'] = nol_usage['rw_twh'].clip(0.0, 2.95)
    
    #get homeusage
    nol_usage['homeusage'] = 3 - nol_usage['rw_twh'].astype(int)
    
    nol_usage.loc[ nol_usage['duration'].isnull(), 'homeusage'] = 0
    
    nol_usage['homeusage'] = nol_usage['homeusage'].fillna(0)
    
    return nol_usage
    
    
def adjustOnlineWeights(npm_hhp_cc, nol_cc_df):
    """
    :param npm_hhp_cc:
    :param nol_cc_df:
    :return:
    """
    #we override nol cc values with npm cc values if it isn't in npm_hhp_cc
    online_w_npmcc = pd.merge( nol_cc_df, npm_hhp_cc[['rn_id', 'cc']].rename( columns = {'cc' : 'npm_cc'}),
                              on = 'rn_id', how = 'left')
    
    indices = online_w_npmcc['npm_cc'].isnull()
    
    online_w_npmcc['cc1'] = online_w_npmcc['npm_cc']
    online_w_npmcc.loc[indices, 'cc1'] = online_w_npmcc.loc[indices, 'cc']
    
    #now we update NPM weights
    npm_cc_sum = npm_hhp_cc.groupby('cc')['weight'].sum().reset_index().rename( columns = {'weight' : 'sum_npm_weight'})
    
    onl_cc = online_w_npmcc.groupby('cc1')['weight'].sum().reset_index().rename( columns = { 'cc1' : 'cc', 'weight' : 'sum_onl_weight'})
    
    npm_nol_cc = pd.merge( npm_cc_sum, onl_cc, on = 'cc')
    
    npm_nol_cc['ratio'] = npm_nol_cc['sum_npm_weight'] / npm_nol_cc['sum_onl_weight']
    
    onl_weight_update_npm = pd.merge(npm_hhp_cc[['respondentid', 'rn_id','age', 'cc', 'weight', 'onl_weight']], 
                                 npm_nol_cc[['cc', 'ratio']], on = 'cc')
    
    onl_weight_update_npm['onl_weight'] = onl_weight_update_npm[['onl_weight', 'ratio']].prod(axis = 1)
    
    #we make sure there is internet useage
    onl_weight_update_npm = onl_weight_update_npm.loc[onl_weight_update_npm['onl_weight'] > 0, :].reset_index(drop = True)
    
    indices = onl_weight_update_npm['weight'] > onl_weight_update_npm['onl_weight']
    
    onl_weight_update_npm['remove_recipient'] = "No"
    onl_weight_update_npm['remove_donor'] = "No"
    
    onl_weight_update_npm.loc[indices, 'remove_recipient'] = "Yes"
    onl_weight_update_npm.loc[~indices, 'remove_donor'] = "Yes"
    
    onl_weight_update_npm['weight_update'] = np.abs( onl_weight_update_npm['weight'] - onl_weight_update_npm['onl_weight'] )
    
    #now we update the NOL weights
    onl_weight_update_nol = pd.merge( npm_nol_cc, npm_nol_cc, how = 'left', on = 'cc' )
    
    onl_weight_update_nol['onl_weight_new'] = onl_weight_update_nol[['weight', 'ratio']].prod(axis = 1)
    
    return onl_weight_update_npm, onl_weight_update_nol

def buildRecipIHDataset(nol_recoded_df, top_nol_200_df, top_strm_50_df,
                          recips, onl_weight_update_npm, linkage_ih_id_nofuse,
                          nol_brand_c_df, nol_channel_c_df, funct_Pivot):
    """
    :param nol_cc_df:
    :param nol_sample_df:
    :param top_nol_200_df:
    :param top_strm_50_df:
    :param recips:
    :param onl_weight_update_npm:
    :param linkage_ih_id_nofuse:
    :param nol_brand_c_df:
    :param nol_channel_c_df:
    :param funct_Pivot:
    :return:
    """
    
    #get all the NOL usage data
    #this has cc information
    pIh_recips_df = pd.merge( pd.merge(recips, 
                                    top_nol_200_df, 
                                    how = 'left', on = 'rn_id'), top_strm_50_df, 
    how = 'left', on = 'rn_id' ).fillna(0.0)

    pIh_recips_df = pd.merge(pIh_recips_df, nol_recoded_df, on = 'rn_id')
    
    nol_usage_brand = pd.merge(pIh_recips_df[['cc', 'weight', 'rn_id']], 
                               nol_brand_c_df[['rn_id', 'category_id', 'subcategory_id', 'duration']], on = 'rn_id').fillna(0)
    
    nol_usage_brand['brand_cs_id'] = nol_usage_brand[['category_id', 'subcategory_id']].apply(lambda x : '_'.join(['brand_c', str(x[0]), 's', str(x[1])]))
    
    nol_usage_channel = pd.merge(pIh_recips_df[['cc', 'weight', 'rn_id']], 
                               nol_channel_c_df[['rn_id', 'category_id', 'subcategory_id', 'duration']], on = 'rn_id').fillna(0)
    
    nol_usage_channel['brand_cs_id'] = nol_usage_channel[['category_id', 'subcategory_id']].apply(lambda x : '_'.join(['channel_c', str(x[0]), 's', str(x[1])]))
    
    nol_usage_brand = getHomeUsage( nol_usage_brand )
    
    nol_usage_channel = getHomeUsage( nol_usage_channel )
    
    nol_brand_cat_piv = funct_Pivot(nol_usage_brand, 'homeusage', 'rn_id', 'brand_cs_id').to_dense().reset_index().rename( columns = { 'index' : 'rn_id' } )
    
    nol_channel_cat_piv = funct_Pivot(nol_usage_channel, 'homeusage', 'rn_id', 'brand_cs_id').to_dense().reset_index().rename( columns = { 'index' : 'rn_id' } )
    
    pIh_recips_df = pd.merge( pIh_recips_df, 
                             pd.merge( pd.merge( pIh_recips_df[['rn_id']], 
                                                nol_brand_cat_piv, 
                                                on = 'rn_id', how = 'left' ),
    nol_channel_cat_piv, on = 'rn_id', how = 'left' ).fillna(0) )
    
    #get the recipients we are going to remove
    indices = onl_weight_update_npm['remove_recipient'] == 'Yes'
    
    indices = pIh_recips_df['rn_id'].isin(onl_weight_update_npm.loc[indices, 'rn_id'] )
    
    recips_1_a = pIh_recips_df.loc[~indices, :].reset_index(drop = True)
    
    recips_1_a['CPH'] = 0
    recips_1_a['new_rn_id'] = recips_1_a['rn_id']
    recips_1_a['new_weight'] = recips_1_a['weight']
    
    recips_1_b = pd.merge( pIh_recips_df, 
                          linkage_ih_id_nofuse.rename( columns = {'weight' : 'new_weight'} ), 
                          on = 'rn_id' )
    
    recips_1_b['CPH'] = 1
    recips_1_b['new_rn_id'] = recips_1_a['rn_id'] + 99
    
    #we make our recipient lists
    recips = pd.concat( [recips_1_a, recips_1_b ], ignore_index = True )
    
    recips['save_weight'] = recips['weight']
    
    recips['weight'] = round(recips['weight']/100,1)
    
    recips = recips.loc[ recips['weight'] > 0, : ].reset_index(drop = True)
        
    internet_home = recips[['cc', 'rn_id', 'new_rn_id', 'new_weight']].rename( columns = {'new_weight' : 'weight'})
    
    return pIh_recips_df, internet_home, nol_brand_cat_piv, nol_channel_cat_piv

def buildDonorDataset(cph_linkage_df, npm_df, hhp_nol_df, npm_hhp_cc, donors, 
                      pIh_recips_df, onl_weight_update_npm, linkage_ih_id_nofuse, nol_brand_cat_piv, 
                      nol_channel_cat_piv, tv_top_100_df):
    """
    :param cph_linkage_df:
    :param npm_df:
    :param hhp_nol_df:
    :param npm_hhp_cc:
    :param donors:
    :param pIh_recips_df:
    :param onl_weight_update_npm:
    :param linkage_ih_id_nofuse:
    :param nol_brand_cat_piv:
    :param nol_channel_cat_piv:
    :param tv_top_100_df:
    :return:
    """
    #we concat data based on missing rn_id's
    missing_rn_id = donors['rn_id'].isnull()
    
    temp = pd.concat( [ donors.loc[~missing_rn_id, :].assign(donorid = donors['respondentid']).drop('weight', axis = 1),
                pd.merge(donors.loc[missing_rn_id, :].drop('weight', axis = 1), 
                         cph_linkage_df.rename(columns = {'recipientid' : 'respondentid'}), on = 'respondentid') ],
                         ignore_index = True )
    
    temp = pd.merge( temp, hhp_nol_df.rename(columns = {'respondentid' : 'donorid',
                                                        'rn_id' : 'rn_id_d'}),
                    on = 'donorid', how = 'left')
    
    #we replace any missing rn_id's with those from the hhp_nol list
    indices = temp['rn_id'].isnull()
    
    temp['rn_id'] = temp.loc[indices, 'rn_id_d']
    
    keep_cols = [col for col in pIh_recips_df.columns if ( 'brand' in col | 'parent' in col | 'channel' in col )]
    
    keep_cols += ['rn_id']
    
    pIh_donors = pd.merge( temp, pIh_recips_df[keep_cols], on = 'rn_id', how = 'left')
    
    #we assume we terr1-6 as features, and cs, which is county size
    #here we add pc and tv data
    pIh_donors = pd.merge( pd.merge( pIh_donors, npm_df.drop('weight', axis = 1), on = 'respondentid', how = 'left' ),
                          tv_top_100_df, on = 'respondentid', how = 'left' )
    
    #here we add nol brand/channel usage data
    pIh_donors = pd.merge( pIh_donors, 
             pd.merge( pd.merge( pIh_donors[['respondentid', 'rn_id']], nol_brand_cat_piv, 
                      on = 'rn_id', how = 'left' ), nol_channel_cat_piv, on = 'rn_id', how = 'left' ).fillna(0),
                                on = 'respondentid' )
             
    pIh_donors = pIh_donors.loc[ pIh_donors['new_weight'] > 0, :].reset_index(drop = True)
    
    pIh_donors['weight'] = pIh_donors['new_weight']
    
    pIh_donors = pIh_donors.drop( ['donorid', 'new_weight'], axis = 1 )
    
    pIh_donors['weight'] = round(pIh_donors['weight']/100).clip(lower = 1.0)
    
    indices = onl_weight_update_npm['remove_donor'] == "Yes"
    
    pIh_donors = pIh_donors.loc[~pIh_donors['respondentid'].isin(onl_weight_update_npm.loc[indices, 'respondentid']), :].reset_index(drop = True)
    
    return pIh_donors

def buildRecipDonorIHDataset(cph_linkage_df, npm_df, hhp_nol_df, npm_hhp_cc, 
                             nol_cc_df, nol_recoded_df,
                               top_nol_200_df, top_strm_50_df,
                               nol_category_brand_df, nol_category_channel_df,
                               tv_top_100_df, funct_Pivot):
    """
    :param cph_linkage_df:
    :param npm_df:
    :param hhp_nol_df:
    :param npm_hhp_cc:
    :param nol_cc_df:
    :param nol_sample_df:
    :param top_nol_200_df:
    :param top_strm_50_df:
    :param nol_category_brand_df:
    :param nol_category_channel_df:
    :param tv_top_100_df:
    :param funct_Pivot:
    :return:
    """
    #we update our weights
    onl_weight_update_npm, onl_weight_update_nol = adjustOnlineWeights(npm_hhp_cc, nol_cc_df)
    
    #create a linkage file that identifies which donors/recipients we will not
    #fuse
    com_cols = ['respondentid', 'rn_id']
    
    linkage_ih_id_nofuse = pd.concat( [onl_weight_update_npm.loc[onl_weight_update_npm['remove_recipient'] == "Yes", com_cols + ['weight']]\
                .rename(columns = {'respondentid' : 'donorid', 'rn_id' : 'recipientid'} ),
                onl_weight_update_npm.loc[onl_weight_update_npm['remove_donor'] == "Yes", com_cols + ['onl_weight']]\
                .rename(columns = {'respondentid' : 'donorid', 'rn_id' : 'recipientid', 'onl_weight' : 'weight'})], 
        ignore_index = True)
    
    #gets the donors
    donors = pd.merge( npm_hhp_cc.loc[npm_hhp_cc['cc'] > 0, :].reset_index(drop = True),
                      onl_weight_update_npm[['respondentid', 'weight_update']], on = 'respondentid', how = 'left' )
    
    donors['new_weight'] = donors[['weight_update', 'weight']].apply(lambda x : x[1] if np.isnan(x[0]) else x[0], axis = 1)
    
    #get the recipients we are going to remove
    indices = onl_weight_update_npm['remove_recipient'] == 'Yes'
    
    indices = onl_weight_update_nol['rn_id'].isin(onl_weight_update_npm.loc[indices, 'rn_id'] )
    
    #we make our recipient lists
    recips = pd.merge( onl_weight_update_nol.loc[~indices, :].reset_index(drop = True),
                      onl_weight_update_npm[['rn_id', 'weight_update']], on = 'rn_id', how = 'left' )
    
    #this has cc information
    recips['new_weight'] = recips[['weight_update', 'onl_weight_new']].apply(lambda x : x[1] if np.isnan(x[0]) else x[0], axis = 1)
    
    indices = onl_weight_update_npm['remove_recipient'] == 'Yes'
    
    recips = pd.concat([recips,
                        onl_weight_update_nol[indices, :].reset_index(drop = True) ], ignore_index = True)
    
    #we get unique category brand information
    nol_brand_c_df = nol_category_brand_df.loc[nol_category_brand_df['tree_level'].upper() == 'S', :]#.drop_duplicates()
    
    #this builds the recipient dataset and returns the internet_home datasets for our fusion
    pIh_recips_df, internet_home, nol_brand_cat_piv, nol_channel_cat_piv = buildRecipIHDataset(nol_recoded_df,
                                                                                               top_nol_200_df, top_strm_50_df,
                                                                                               recips, onl_weight_update_npm, linkage_ih_id_nofuse,
                                                                                               nol_brand_c_df, nol_category_channel_df, funct_Pivot)
    
    pIh_donors_df = buildDonorDataset(cph_linkage_df, npm_df, hhp_nol_df, npm_hhp_cc, donors, 
                      pIh_recips_df, onl_weight_update_npm, linkage_ih_id_nofuse, nol_brand_cat_piv, 
                      nol_channel_cat_piv, tv_top_100_df)
    
    return pIh_donors_df, pIh_recips_df, internet_home, donors['weight'].sum(), linkage_ih_id_nofuse