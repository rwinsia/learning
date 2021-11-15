import pandas as pd
import numpy as np
from sklearn.model_selection import cross_val_score, StratifiedShuffleSplit, RandomizedSearchCV, GridSearchCV
from sklearn.metrics import roc_auc_score, precision_score, recall_score, f1_score, auc, roc_curve
from sklearn.base import clone
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
import catboost as cgb
import xgboost as xgb
from xgboost import XGBClassifier
from catboost import Pool, CatBoostClassifier
from scipy import interp
import warnings
import pickle
import os
import IPython
import json
import matplotlib.pyplot as plt
import enum
from scorecard.binning_builder import BinningBuilder

class IndodanaScorecard(enum.Enum):
	"""
	PDO: float
		 Point to double the odds value for score scaling process
		 Default: 40
	ODDS: float
		 Odds for score scaling process, usually 50 good contracts per 1 bad contracts at desired score (S0)
		 Default: 50
	S0: float
		 Score for certain odds used in score scaling process, 500 means that at that score, the odds will be 50:1
		 Default: 500
	"""

	PDO = 40
	ODDS = 50
	S0 = 500



class ModelData(dict):
	def __getattr__(self, name):
		return self[name]

	def __getstate__(self):
		return self.__dict__

	def __setstate__(self, state):
		self.__dict__ = state


class Scorecard(object):
	"""
	Scorecard(data, cv=None, key='orderId', flag='flag')

	A class used for creating score cards mainly used for Indodana's credit scoring.

	Parameter
	---------
	data : pandas.DataFrame
		DataFrame that contains main data used for training the model
	cv : Cross Validation model for training
		Default: StratifiedShuffleSplit(n_splits=5,random_state=0)
	key : str
		Unique identifier for the data
	flag: str
		Column for the target variable
	"""
	def __init__(self, data, cv=None, key='orderId', flag='flag'):
		if not np.issubdtype(data[flag].dtype, np.number):
			raise TypeError("Target data must be numeric")
		self.default_metrics = ['roc_auc', 'precision', 'recall', 'f1', 'conf_matrix', 'score_metrics']
		self.model = None
		if cv is None:
			cv = StratifiedShuffleSplit(n_splits=5,random_state=0)
		self.cv = cv

		self.data = data
		self.intime = ModelData()
		self.oot = ModelData()

		self.variables = None
		self.default_woe = 0 
		self.meta_data = {} 
		self.pivot_tables = {}
		self.key = key
		self.flag = flag

		self._calculate_constants()
		self.base_point = None

	def __getstate__(self):
		return self.__dict__

	def __setstate__(self, state):
		self.__dict__ = state

	def load_from_pickle(self, filename):
		pickle_in = open(filename,"rb")
		self = pickle.load(pickle_in)

	def save_to_pickle(self, filename):
		pickle_out = open(filename,"wb")
		pickle.dump(self, pickle_out)


	def load_meta_data_from_dict(self, meta_data):
		self.meta_data = meta_data

	def load_meta_data_from_file(self, path):
		meta_data = BinningBuilder.load_meta_data_from_file(path)
		self.meta_data = meta_data

	def _calculate_constants(self):
		self.M = IndodanaScorecard.PDO.value / np.log(2.)
		self.C = IndodanaScorecard.S0.value + (self.M * np.log(1./IndodanaScorecard.ODDS.value))

	def get_numerical_data_frame(self, variables):
		self.woe_transform(variables, auto_binning=True)
		intime = self.intime['data'][[self.key, self.flag] + variables].copy()
		oot = self.oot['data'][[self.key, self.flag] + variables].copy()

		for col in variables:
			if self.meta_data[col]["data_type"] != "numerical":
				intime[col] = self.intime['data_woe'][col]
				oot[col] = self.oot['data_woe'][col]
			else:
				default_null = self.meta_data[col]["default_null"]
				intime[col] = intime[col].fillna(default_null)
				oot[col] = oot[col].fillna(default_null)

		return intime, oot
		
	def woe_transform(self, variables, auto_binning=False, default_woe=0):
		self.intime['data_woe'] = self.intime['data'][[self.key, self.flag]]
		self.oot['data_woe'] = self.oot['data'][[self.key, self.flag]]
		self.intime['data_bin'] = self.intime['data'][[self.key, self.flag]]
		self.oot['data_bin'] = self.oot['data'][[self.key, self.flag]]
		self.intime['mapper'] = {}
		self.variables = variables
	
		for col in variables:
			if col not in self.meta_data:
				if auto_binning:
					self.meta_data[col] = BinningBuilder.auto_binning(self.intime['data'][col])
				else:
					raise ValueError("Meta data information on binning & default values are not defined for %s"%col)
			self.intime['data_bin'][col] = BinningBuilder.get_binning(self.intime['data'][col], self.meta_data[col])
			self.oot['data_bin'][col] = BinningBuilder.get_binning(self.oot['data'][col], self.meta_data[col])
			self.intime['mapper'][col] = BinningBuilder.generate_woe_mapper(self.intime['data_bin'], col, self.flag, key=self.key)
			self.intime['data_woe'][col] = self.bin_to_woe(self.intime['data_bin'][col], self.intime['mapper'][col], col)
			self.oot['data_woe'][col] = self.bin_to_woe(self.oot['data_bin'][col], self.intime['mapper'][col], col, verbose=True)

	def intime_oot_split(self, start_date_intime, end_date_intime, start_date_oot, end_date_oot, date_column="approvedDate"):
		"""
		Function to generate intime dataset and out of time dataset by splitting the main data

		Generated Attributes
			- intime['data']: pandas.DataFrame, data intime
			- oot['data']: pandas.DataFrame, data out of time (oot)

		Parameter
		---------
		date_column: str, date data column in data
		start_date_intime: datetime, start date for intime dataset
		end_date_intime: datetime, end date for intime dataset
		start_date_oot: datetime, start date for out of time dataset
		end_date_oot: datetime, end date for out of time dataset
		"""

		self.intime['start_date'] = start_date_intime
		self.intime['end_date'] = end_date_intime
		self.oot['start_date'] = start_date_oot
		self.oot['end_date'] = end_date_oot
		self.intime['data'] = self.data[(self.data[date_column] >= start_date_intime) & (self.data[date_column] <= end_date_intime)]
		self.oot['data'] = self.data[(self.data[date_column] >= start_date_oot) & (self.data[date_column] <= end_date_oot)]

	def get_prediction(self, series, threshold_value, threshold_type):
		"""
		Get prediction (binary) given a probability or score dataset and threshold value.

		Parameter
		---------
		- series: pandas.Series, score or probability series that will be transformed into binary class
		- threshold_value: float, score or probability threshold
		- threshold_type: str, options: score, probability.

		Return
		---------
		pandas.Series, predicted class data
		"""
		self._check_threshold_type(threshold_type)

		if threshold_type == 'score':
			return series.apply(lambda x: self._score_threshold(x, threshold_value))
		elif threshold_type == 'probability':
			return series.apply(lambda x: self._proba_threshold(x, threshold_value))

	def score_distribution(self, score, target, interval=10., min_score=300., max_score=480., threshold_value=330., name='model'):
		"""
		Create score distribution for model evaluation

		Parameter
		---------
		- score: pandas.Series, data series to be grouped
		- target: pandas.Series, target data series to create bad rate for every score bands
		- interval: float, bins interval, default: 20.

		Optional Parameter
		- min_score: float, upper boundary of lowest class (lowest class will be -np.inf to min_score)
					default: 300.
		- max_score: float, lower boundary of highest class (will be max_score to np.inf)
					default: 480.
		- threshold_value: float, score threshold to calculate population above and below threshold
					default: 330.

		Return
		------
		- score_table: pandas.DataFrame, score distribution pivot table containing
					   BADRATE, DIST_BAD, DIST_GOOD, DIST
		- distribution_metrics: dict, dictionary containing score metrics containing
					 name: dictionary name
					 badrate: bad rate in total population
					 population_above_threshold: % population above score threshold
					 population_below_threshold: % population below score threshold
					 cumulative_bad_rate_above_threshold: cumulative bad rate above score threshold
					 threshold: threshold value

		"""
		bins = np.arange(min_score, max_score, interval)
		bins = np.insert(bins, [0, len(bins)], [-np.inf, np.inf])
		score_table = pd.crosstab(pd.cut(score, bins, include_lowest=True), target).reset_index()
		score_table.loc[:, 'Total']= score_table[[0,1]].sum(axis=1)
		score_table.loc['Total', :]= score_table.sum(axis=0)
		score_table['BADRATE'] = score_table[1] / (score_table[1]+score_table[0])
		score_table['DIST_BAD'] = score_table[1] / score_table[1].sum()
		score_table['DIST_GOOD'] = score_table[0] / score_table[0].sum()
		score_table['DIST'] = score_table['Total'] / score_table['Total'].loc['Total']
		bad_rate = target.sum() / target.count()
		target_above = target[score >= threshold_value]
		pop_above = score[score >= threshold_value].count() / score.count()
		pop_below = 1 - pop_above
		bad_rate_above = target_above.sum() / target_above.count()
		distribution_metrics = {'name': name,
								'badrate': bad_rate,
								'population_above_threshold': pop_above,
								'population_below_threshold': pop_below,
								'cumulative_bad_rate_above_threshold': bad_rate_above,
								'threshold': threshold_value}

		return score_table, distribution_metrics


	def train(self, model, params={}, cv_scoring='roc_auc', woe=False, score_card=True, verbose=True):
		"""
		Train the model with defined cross validation object.
		Please call get_dataset function first before using this method

		Generated attributes
			- model: fitted model object state
			- intime['cv_scoring']: str, cross validation scoring type
			- intime['cv_scores']: list, cross validation scores
			- intime['woe_pivot']:  pandas.DataFrame, pivot table
			- intime['trained_data']: pandas.DataFrame, intime main data merged with data used for training (intime.X)
									  there will be '_unused' suffix in this dataset to mark that the data were not used in training
			- oot['trained_data']: pandas.DataFrame, out of time main data merged with data used for training (oot.X)
								   there will be '_unused' suffix in this dataset to mark that the data were not used in training


		Parameter
		---------
		- model: estimator model object that contains .fit, .predict, and .predict_proba methods
		- cv_scoring: 'str', cross validation scoring method
					  using sklearn.model_selection.cross_val_score
					  default: roc_auc

		"""
		self.model = clone(model)

		for col in self.variables + [self.flag]:
			if self.intime.data_woe[col].isnull().any() == True:
				print("The data " + col + " contain missing values and missing values will be handled using missing handler defined at transformer creation.")

		# cross validation
		cv_scores = cross_val_score(estimator=self.model, X=self.intime.data_woe[self.variables],
                                                 y=self.intime.data_woe[self.flag], scoring='roc_auc', cv=self.cv)
		cv_mean = cv_scores.mean()
		self.intime['evaluation'] = {'CROSS VALIDATION': cv_mean}
		self.oot['evaluation'] = {'CROSS VALIDATION': ''}
		print(f'Intime AUC: {cv_scores}, mean: {cv_mean}')


		# model training
		self.model.fit(self.intime.data_woe[self.variables], self.intime.data_woe[self.flag])
		if self.model.coef_ is None:
			raise TypeError("Model has no coefficient variable, scorecard cannot be produced.")
		coefs = self.model.coef_.ravel()
		
		# scorecard
		self.pivot_tables = {}
		for i, col in enumerate(self.variables):
			print(col)
			intime_pivot = self._generate_pivot_table(self.intime, col)
			oot_pivot = self._generate_pivot_table(self.oot, col, score=True)

			pivot = intime_pivot.join(oot_pivot, how='left', lsuffix='_intime', rsuffix='_oot')
			pivot = pivot.reset_index()
			pivot["SCORE"] = round(coefs[i] * pivot[col+"_WOE"] * self.M * -1)
			self.pivot_tables[col] = pivot 
			if coefs[i] < 0:
				print("Warning: Negative coefficient for %s"%col)
			display(pivot)

		if self.model.intercept_ is None:
			raise TypeError("Model has no intercept, base score cannot be calculated.")
		b0 = self.model.intercept_[0]
		base_point = self.C - b0 * self.M
		print("Model base point: %3f"%base_point)
		self.base_point = base_point

		# evaluation
		y_proba_intime = self.model.predict_proba(self.intime.data_woe[self.variables])[:, 1]
		y_proba_oot = self.model.predict_proba(self.oot.data_woe[self.variables])[:, 1]


		self.intime['evaluation']['AUC'] = roc_auc_score(self.intime.data_woe[self.flag], y_proba_intime)
		self.oot['evaluation']['AUC'] = roc_auc_score(self.oot.data_woe[self.flag], y_proba_oot)

		self.intime['evaluation']['start_date'] = self.intime['start_date'] 
		self.intime['evaluation']['end_date'] = self.intime['end_date'] 

		self.oot['evaluation']['start_date'] = self.oot['start_date'] 
		self.oot['evaluation']['end_date'] = self.oot['end_date'] 

		y_score_intime = self.map_to_score(y_proba_intime)
		y_score_oot = self.map_to_score(y_proba_oot)

		self.intime['evaluation']['gte_330'] = (y_score_intime >= 330).astype(int).sum()
		self.intime['evaluation']['lt_330'] = (y_score_intime < 330).astype(int).sum()

		self.oot['evaluation']['gte_330'] = (y_score_oot >= 330).astype(int).sum()
		self.oot['evaluation']['lt_330'] = (y_score_oot < 330).astype(int).sum()

		self.intime['evaluation']['percentage_gte_330'] = self.intime['evaluation']['gte_330']/self.intime.data.shape[0] 
		self.oot['evaluation']['percentage_gte_330'] = self.oot['evaluation']['gte_330']/self.oot.data.shape[0] 

		intime_metrics = pd.DataFrame(self.intime['evaluation'], index=['intime'])
		oot_metrics = pd.DataFrame(self.oot['evaluation'], index=['oot'])
		self.evaluation = intime_metrics.append(oot_metrics)
		self.evaluation = self.evaluation[['AUC', 'CROSS VALIDATION', 'start_date', 'end_date', 'gte_330', 'lt_330', 'percentage_gte_330']]

	def predict_proba(self, data, woe=True, verbose=True):
		data_bin = pd.DataFrame()
		data_woe = pd.DataFrame()
	
		for col in self.variables:
			if self.meta_data[col]['data_type'] == "categorical":
				data[col] = data[col].astype(str)
			if verbose:
				print("Binning and calculating woe for %s"%col)
			data_bin[col] = BinningBuilder.get_binning(data[col], self.meta_data[col])
			mapper = self.intime['mapper'][col]
			data_woe[col] = self.bin_to_woe(data_bin[col], mapper, col)

		return self.model.predict_proba(data_woe)

	def predict_score(self, data, verbose=True):
		probabilities = pd.Series(self.predict_proba(data[self.variables], verbose=verbose)[:, 1])
		score = self.map_to_score(probabilities)
		return score

	def map_to_score(self, probabilities):
		score = pd.Series(probabilities).apply(lambda probability: self.C - (self.M * np.log(probability / (1 - probability))))
		return score

	@staticmethod
	def map_to_score(probabilities):    
		M = IndodanaScorecard.PDO.value / np.log(2.)
		C = IndodanaScorecard.S0.value + (M * np.log(1./IndodanaScorecard.ODDS.value))  
		score = pd.Series(probabilities).apply(lambda probability: C - (M * np.log(probability / (1 - probability))))
		return score

	def set_binning(self, col, binning_data):
		self.meta_data[col]['binning_data'] = binning_data

	def generate_score_distribution(self, data=None, by_month=False, date_column=None, binning=20):
		if by_month and date_column is None:
			raise ValueError("Parameter date_column must defined for by_month pivot")
		if data is None:
			data = self.data.copy()

		indexes = ['score_bin']
		if by_month:
			data['month'] = pd.to_datetime(data[date_column]).dt.month 
			indexes = ['month', 'score_bin']
		data['score'] = self.predict_score(data)	
		data['score_bin'] = pd.cut(data['score'], range(0, 1000, binning))
		pivot = pd.pivot_table(data, index=['score_bin'], values=self.key, columns=[self.flag], aggfunc='count').fillna(0)
		pivot['TOTAL'] = pivot[0] + pivot[1]
		pivot['BADRATE'] = pivot[1]/pivot['TOTAL']
		pivot["CUMSUM"] = pivot["TOTAL"].cumsum()/pivot["TOTAL"].sum()
		return pivot
			
	def _generate_pivot_table(self, dataset, col, score=False):
		merged_data = pd.merge(dataset.data_bin, dataset.data_woe[[self.key] + self.variables], on=self.key, suffixes=['', '_WOE'])
		pivot = pd.pivot_table(merged_data, index=[col, col+'_WOE'], values=self.key, columns=[self.flag], aggfunc='count')
		pivot["TOTAL"] = pivot[0] + pivot[1]
		pivot["BADRATE"] = pivot[1]/pivot["TOTAL"]

		if score:
			temp = merged_data[[col, col+'_WOE']].reset_index()
			temp["model_score"] = self.predict_score(self.oot.data.reset_index(), verbose=False)
			pivot["median_score_oot"] = temp.groupby([col,col+'_WOE'])['model_score'].median()
		
		return pivot

	def bin_to_woe(self, data_bin, mapper, col, verbose=False):
		default_bin = self.meta_data[col]['default_bin']
		if default_bin not in mapper:
			mapper[default_bin] = self.default_woe
		for binning in data_bin.unique():
			if binning not in mapper:
				mapper[binning] = self.default_woe
		data_woe = data_bin.replace(mapper)
		if verbose and data_woe.isna().any():
			print("Warning: Default WOE values used for col %s because some binnings are not available in mapping"%col)
		data_woe = data_woe.replace(np.nan, self.default_woe)
		if verbose and ((data_woe == np.inf) | (data_woe == -np.inf)).any():
			print("Warning: Default WOE values used for col %s because some binnings values are inf"%col)
		data_woe = data_woe.replace(np.inf, self.default_woe)
		data_woe = data_woe.replace(-np.inf, self.default_woe)
		return data_woe

	def save_meta_data_to_file(self, path):
		to_save = self.meta_data.copy()
		for key in to_save:
			binning_data = to_save[key]['binning_data']
			to_save[key]['binning_data'] = json.dumps(binning_data)
		meta_data = pd.DataFrame(to_save)
		meta_data.to_csv(path)

	def save_meta_data_to_dict(self):
		return self.meta_data

	def save_to_excel(self, path, date_columns=["approvedDate"]):
		"""
		Saves intime data, oot data, scorecard and performance to excel.

		Parameter
		---------
		path (str): Path to the .xlsx file to save the data.
		date_columns (list): Columns to convert to excel date format %m-%d-%Y

		"""
		intime_data = self.intime.data.copy()
		oot_data = self.oot.data.copy()
		evaluation = self.evaluation.copy()

		for date in date_columns:
			intime_data[date] = self.intime.data[date].dt.strftime("%m-%d-%Y")
			oot_data[date] = self.oot.data[date].dt.strftime("%m-%d-%Y")

		evaluation["start_date"] = evaluation["start_date"].dt.strftime("%m-%d-%Y")
		evaluation["end_date"] = evaluation["end_date"].dt.strftime("%m-%d-%Y")
		intime_data["product_type"] = self.intime.data[self.key].str.slice(0,3)
		oot_data["product_type"] = self.oot.data[self.key].str.slice(0,3)

		scorecard = pd.DataFrame()

		for var in self.variables:
			temp_pivot = self.pivot_tables[var].copy()
			temp_pivot['var_name'] = var
			temp_pivot = temp_pivot.rename(columns={var: 'bins'})
			scorecard = scorecard.append(temp_pivot, ignore_index=True)
		
		scorecard = scorecard.append(pd.DataFrame({'var_name': ['base_score'], 'SCORE': self.base_point}), ignore_index=True)
		scorecard = scorecard[['var_name', 'bins','0_intime', '1_intime',
							 'BADRATE_intime', #'DIST_BAD', 'DIST_GOOD', 'WOE', 'weights',
							 '0_oot', '1_oot', 'BADRATE_oot', 'SCORE']]

		writer = pd.ExcelWriter(path,
			options={'remove_timezone': True},
			engine='xlsxwriter')
		
		intime_data.to_excel(writer, sheet_name='intime_data')
		oot_data.to_excel(writer, sheet_name='oot_data')
		scorecard.to_excel(writer, sheet_name='scorecard')
		evaluation.to_excel(writer, sheet_name='model_performance')

		writer.save()
		print(f"Excel saved to {path}")

	@staticmethod
	def _replace_inf(dic):
		for k,v in dic.items():
			if v == np.inf:
				dic[k] = "inf"
			if v == -np.inf:
				dic[k] = "-inf"
		return dic

	def generate_json(self, path, feature_rule_mapping, verbose=False):
		"""
		NOTE: Still work in progress
		"""
		json_dict = {}
		json_dict["rules"] = {}

		for feature_name in self.variables:
			if feature_name in feature_rule_mapping:
				rule_name = feature_rule_mapping[feature_name]

				if rule_name not in json_dict["rules"]:
					json_dict["rules"][rule_name] = {}
				meta_data = self.meta_data[feature_name]
				temp_pivot = self.pivot_tables[feature_name]
				score_list = []
				if meta_data['data_type'] == 'categorical':
					default_assigned = False
					reverse_dic = {}
					for key in meta_data['binning_data']:
						val = meta_data['binning_data'][key]
						if val not in reverse_dic:
							reverse_dic[val] = []
						reverse_dic[val].append(key)
					for key in reverse_dic:
						temp_dic = {}
						row = temp_pivot[temp_pivot[feature_name] == key]
						if row.shape[0] > 0:
							temp_dic['score'] = row['SCORE'].values[0]
							temp_dic['in'] = reverse_dic[key]
							if meta_data['default_bin'] in reverse_dic[key]:
								temp_dic['default'] = True
								default_assigned = True
							score_list.append(temp_dic)
					if not default_assigned:
						score_list.append({'score': self.default_woe, 'in': [meta_data['default_bin']], 'default': True})
				else:
					for i in range(temp_pivot.shape[0]):
						temp_dict = {}
						interval = temp_pivot[feature_name].values[i]
						temp_dict['min'] = interval.left
						temp_dict['max'] = interval.right
						temp_dict['score'] = temp_pivot['SCORE'].values[i]
						score_list.append(temp_dict)
					score_list = [Scorecard._replace_inf(dic) for dic in score_list]
				json_dict["rules"][rule_name][feature_name] = score_list 
			else:
				print("Warning! %s not in feature rule mapping."%feature_name)

		if verbose:
			display(json_dict)
		with open(path, "w") as file:
			json.dump(json_dict, file)
