from __future__ import division
import pandas as pd
import numpy as np
import datetime
import scipy as sc
import time
import datetime
from sklearn.preprocessing import StandardScaler
from sklearn import svm
import gbq_large
import subprocess

def main():

	with open('trained_outliers.pkl', 'rb') as fid:
    	clf2 = cPickle.load(fid)
	print('Predicting Outliers')
	p=clf2.predict(df_train_T)
	d=clf2.decision_function(df_train_T)
	print('Post Processing')
	df_classed = df_train_T.copy()
	df_classed['uid']=df.uid
	df_classed['class'] = p
	df_classed['distance'] = d
	df_classed = df_train_T.copy()
	df_classed['uid']=df.uid
	df_classed['class'] = p
	df_classed['distance'] = d
	df_classed_duration = df_classed[['bin1_duration_win','bin2_duration_win','bin3_duration_win',
	                                'bin4_duration_win','bin5_duration_win','bin6_duration_win'
	                                 ]]
	df_classed_win = df_classed[['bin1_winrate','bin2_winrate','bin3_winrate',
	                                'bin4_winrate','bin5_winrate','bin6_winrate',
	                                 ]]
	df_classed_out=df_classed[((df_classed_win[df_classed_win > 0.001].count(axis=1) - df_classed_win[df_classed_win < -0.001].count(axis=1))>=0) & ((df_classed_duration[df_classed_duration < -0.001].count(axis=1) - df_classed_win[df_classed_win > 0.001].count(axis=1))>=0)]
	df_classed_out_duration = df_classed_out[['bin1_duration_win','bin2_duration_win','bin3_duration_win',
	                                'bin4_duration_win','bin5_duration_win','bin6_duration_win'
	                                 ]]
	df_classed_win_duration = df_classed_out[['bin1_winrate','bin2_winrate','bin3_winrate',
	                                'bin4_winrate','bin5_winrate','bin6_winrate',
	                                 ]]
	df_classed_out['average_duration']=df_classed_out_duration.mean(axis=1)
	df_classed_out = df_classed_out[(df_classed_out['average_duration']<0)]
	df_outliers_svm = df_classed_out[df_classed_out['class']==-1].sort(['distance'])
	df_w_uid = df.copy()
	df_w_uid['uid']=df.uid
	df_w_uid['distance']= d
	df_write =df_w_uid[df_w_uid.uid.isin(df_outliers_svm.uid)].sort(['distance'])
	df_write['priority'] = df_write.distance/df_write.distance.min()
	date=datetime.datetime.now()
	df_write['ts_t']=date.strftime('%Y-%m-%d %H:%M:%S')
	print('Write Outliers to CSV')
	df_write.to_csv('outliers.csv',index=False)
	table_write = 'mcoc-bi:datascience_view.pvp_outliers'+ date.strftime('%Y%m%d')
	print('Upload Outlier Data to BigQuery')
	subprocess.call('''~/google-cloud-sdk/bin/bq load --source_format=CSV --skip_leading_rows=1 ''' +  table_write + ''' outliers.csv uid_i:integer,bin1_winrate_f:float,bin2_winrate_f:float,bin3_winrate_f:float,bin4_winrate_f:float,bin5_winrate_f:float,bin6_winrate_f:float,bin1_duration_win_f:float,bin2_duration_win_f:float,bin3_duration_win_f:float,bin4_duration_win_f:float,bin5_duration_win_f:float,bin6_duration_win_f:float,distance_f:float,priority_f:float,ts_t:timestamp''', shell=True)
	

if __name__ == "__main__":
	main()