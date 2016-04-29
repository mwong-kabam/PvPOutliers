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
import cPickle
import math

def main():
	print('Querying ...')
	query = '''SELECT uid, MAX(bin1_winrate) bin1_winrate, MAX(bin2_winrate) bin2_winrate, 
	MAX(bin3_winrate) bin3_winrate, MAX(bin4_winrate) bin4_winrate, MAX(bin5_winrate) bin5_winrate, 
	MAX(bin6_winrate) bin6_winrate,
	MAX(bin1_duration_win) bin1_duration_win, MAX(bin2_duration_win) bin2_duration_win, 
	MAX(bin3_duration_win) bin3_duration_win, 
	MAX(bin4_duration_win) bin4_duration_win, MAX(bin5_duration_win) bin5_duration_win, 
	MAX(bin6_duration_win) bin6_duration_win
	FROM
	(SELECT uid, 

		(CASE when diff_bin = 1 AND n_matches > 4 THEN win_rate ELSE null end) as bin1_winrate,
		(CASE when diff_bin = 2 AND n_matches > 4 THEN win_rate ELSE null end) as bin2_winrate,
		(CASE when diff_bin = 3 AND n_matches > 4 THEN win_rate ELSE null end) as bin3_winrate,
		(CASE when diff_bin = 4 AND n_matches > 4 THEN win_rate ELSE null end) as bin4_winrate,
		(CASE when diff_bin = 5 AND n_matches > 4 THEN win_rate ELSE null end) as bin5_winrate,
		(CASE when diff_bin = 6 AND n_matches > 4 THEN win_rate ELSE null end) as bin6_winrate,

		(CASE when diff_bin = 1 THEN integer(n_matches) ELSE 0 end) as bin1_nmatches,
		(CASE when diff_bin = 2 THEN integer(n_matches) ELSE 0 end) as bin2_nmatches,
		(CASE when diff_bin = 3 THEN integer(n_matches) ELSE 0 end) as bin3_nmatches,
		(CASE when diff_bin = 4 THEN integer(n_matches) ELSE 0 end) as bin4_nmatches,
		(CASE when diff_bin = 5 THEN integer(n_matches) ELSE 0 end) as bin5_nmatches,
		(CASE when diff_bin = 6 THEN integer(n_matches) ELSE 0 end) as bin6_nmatches,

		(CASE when diff_bin = 1 AND n_wins > 4 THEN avg_duration_wins ELSE null end) as bin1_duration_win,
		(CASE when diff_bin = 2 AND n_wins > 4 THEN avg_duration_wins ELSE null end) as bin2_duration_win,
		(CASE when diff_bin = 3 AND n_wins > 4 THEN avg_duration_wins ELSE null end) as bin3_duration_win,
		(CASE when diff_bin = 4 AND n_wins > 2 THEN avg_duration_wins ELSE null end) as bin4_duration_win,
		(CASE when diff_bin = 5 AND n_wins > 2 THEN avg_duration_wins ELSE null end) as bin5_duration_win,
		(CASE when diff_bin = 6 AND n_wins > 2 THEN avg_duration_wins ELSE null end) as bin6_duration_win,

		FROM

		(SELECT uid,
			(CASE WHEN differential BETWEEN 1.0 AND 1.5 THEN 1 
				WHEN differential BETWEEN 1.5 AND 2.0 THEN 2 
				WHEN differential BETWEEN 2.0 AND 2.5 THEN 3 
				WHEN differential BETWEEN 2.5 and 4.0 THEN 4 
				WHEN differential BETWEEN 4.0 AND 6.0 THEN 5  
				WHEN differential > 6.0 then 6 END) AS diff_bin, 
COUNT(*) as n_matches,
SUM(wins)/COUNT(*) as win_rate,
AVG(data_game_stats_fight_duration_i) as avg_duration,
AVG(duration_wins) as avg_duration_wins,
AVG(player_hits) AS avg_player_hits,
AVG(player_blocks) AS avg_player_blocks,
AVG(player_damage/player_hits) AS avg_player_damage,
AVG(opponent_hits) AS avg_opponent_hits,
AVG(opponent_damage/opponent_hits) AS avg_opponent_damage,
AVG(opponent_blocks) AS avg_opponent_blocks,
SUM(ifwin) AS n_wins

FROM (
	SELECT
	ts_t,
	data_match_data_opponent_pi_i / (data_match_data_unit_pi_i*1.00) AS differential,
	uid_i AS uid,
	data_game_stats_ai_profile_name_s AS ai_profile,
	data_match_data_unit_pi_i,
	data_match_data_opponent_pi_i,
	data_match_data_unit_name_s AS unit,
	data_match_data_opponent_name_s AS opponent,
	data_game_stats_player_0_stats_hits_thrown_i AS player_hits,
	data_game_stats_player_0_stats_blocks_i AS player_blocks,
	data_game_stats_player_0_stats_damage_dealt_i AS player_damage,
	data_game_stats_player_1_stats_hits_thrown_i AS opponent_hits,
	data_game_stats_player_1_stats_damage_dealt_i AS opponent_damage,
	data_game_stats_player_1_stats_blocks_i AS opponent_blocks,
	CASE WHEN RIGHT(data_match_data_unit_name_s,2) = 'cm'
	AND RIGHT(data_match_data_opponent_name_s,2)='ep' THEN 3 WHEN RIGHT(data_match_data_unit_name_s,2) = 'un'
	AND RIGHT(data_match_data_opponent_name_s,2)='ep' THEN 2 WHEN RIGHT(data_match_data_unit_name_s,2) = 'ar'
	AND RIGHT(data_match_data_opponent_name_s,2)='ep' THEN 1 WHEN RIGHT(data_match_data_unit_name_s,2) = 'cm'
	AND RIGHT(data_match_data_opponent_name_s,2)='ar' THEN 2 WHEN RIGHT(data_match_data_unit_name_s,2) = 'un'
	AND RIGHT(data_match_data_opponent_name_s,2)='ar' THEN 1 WHEN RIGHT(data_match_data_unit_name_s,2) = 'cm'
	AND RIGHT(data_match_data_opponent_name_s,2)='un' THEN 1 WHEN RIGHT(data_match_data_unit_name_s,2) = 'ep'
	AND RIGHT(data_match_data_opponent_name_s,2)='ar' THEN -1 WHEN RIGHT(data_match_data_unit_name_s,2) = 'ep'
	AND RIGHT(data_match_data_opponent_name_s,2)='un' THEN -2 WHEN RIGHT(data_match_data_unit_name_s,2) = 'ep'
	AND RIGHT(data_match_data_opponent_name_s,2)='cm' THEN -3 WHEN RIGHT(data_match_data_unit_name_s,2) = 'ar'
	AND RIGHT(data_match_data_opponent_name_s,2)='un' THEN -1 WHEN RIGHT(data_match_data_unit_name_s,2) = 'ar'
	AND RIGHT(data_match_data_opponent_name_s,2)='cm' THEN -2 WHEN RIGHT(data_match_data_unit_name_s,2) = 'un'
	AND RIGHT(data_match_data_opponent_name_s,2)='cm' THEN -1 ELSE 0 END AS rarity_delta,
	(CASE WHEN data_outcome_s='WON' THEN 1 ELSE 0 END) AS wins,
	(CASE WHEN data_outcome_s='LOST' THEN 1 ELSE 0 END) AS losses,
	data_game_stats_player_1_stats_special3_procs_i + data_game_stats_player_1_stats_special2_procs_i + data_game_stats_player_1_stats_special1_procs_i AS opponent_procs,
	data_game_stats_fight_duration_i,
	(CASE WHEN data_outcome_s='WON' THEN data_game_stats_fight_duration_i ELSE null END) AS duration_wins,
	(CASE WHEN data_outcome_s='WON' THEN 1 ELSE 0 END) AS ifwin
	FROM
	TABLE_DATE_RANGE(marvel_production_view.match, DATE_ADD(CURRENT_TIMESTAMP(),-5,"day"), CURRENT_TIMESTAMP())
	WHERE
	counter_s LIKE 'pvp%resolve'

	)
GROUP EACH BY 1,2,
ORDER EACH BY 1,2))

GROUP EACH BY 1'''
	
	df=gbq_large.read_gbq(query,project_id='mcoc-bi',destination_table='datascience_view.outliers_tmp')
	print('Finished BigQuery Load')
	df = df.dropna(how='all',subset=['bin1_winrate', 'bin2_winrate', 'bin3_winrate', 'bin4_winrate', 'bin5_winrate', 'bin6_winrate',
		'bin1_duration_win', 'bin2_duration_win', 'bin3_duration_win', 'bin4_duration_win', 'bin5_duration_win', 'bin6_duration_win'])
	df_train = df.drop(['uid'],axis=1)
	df_train = df_train.apply(lambda x: x.fillna(0) if math.isnan(x.mean()) else x.fillna(x.mean())  ,axis=0)
	df_train_T=df_train.apply(lambda x:StandardScaler().fit_transform(x))
	df_train_small=df_train_T.sample(frac=0.8,axis=0)
	clf2 = svm.OneClassSVM(kernel="rbf",cache_size=16000,nu=0.01,gamma=1/14)
	print('Training Model')
	clf2.fit(df_train_small)
	with open('trained_outliers.pkl', 'wb') as fid:
		cPickle.dump(clf2, fid) 
	

if __name__ == "__main__":
	main()