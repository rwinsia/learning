import pandas as pd
import numpy as np
import json
import pickle
import warnings
import matplotlib.pyplot as plt
import plotly.express as px
import datetime
import time
import seaborn as sns

from google.cloud import bigquery

class BinningBuilder(object):
	def __init__(self, data, start_dateintime, end_dateintime, start_date_oot, end_date_oot, date_column='approvedDate', key='orderId', flag='flag'):
		self.data = data
		self.raw_data = data.copy()
		self.data["month"] = pd.to_datetime(self.data[date_column], utc=True).dt.strftime("%Y-%m")
		self.meta_data = {} 
		self.train = None
		self.train_binning = None 
		self.test = None
		self.key = key
		self.flag = flag
		unique_flag = self.data[self.flag].nunique() 
		if unique_flag != 2:
			raise ValueError("The target column must be binary, but it contains %d value(s)."%unique_flag)
		self.start_dateintime = start_dateintime
		self.end_dateintime = end_dateintime
		self.start_date_oot = start_date_oot
		self.end_date_oot = end_date_oot
		self.date_column = date_column
		self.error_handler = {}
		self.intime_oot_split()

	@staticmethod
	def create_binning_from_list(keys, value, existing_binning={}):
		"""
		Reverses one-to-many mappings to be inputted as binning data.

		Parameter
		------
		keys: list, List of strings to be used as keys to the binning dictionary
		value: str, Value to every key in the dictionary
		existing_binning: dict, Existing dictionary to be added key,value pairs if applicable
		"""
		binning_data = existing_binning
		for key in keys:
			binning_data[str(key)] = str(value)
		return binning_data

	@staticmethod
	def get_default_categorical_binning(series):
		"""
		Create default categorical. Example, [a,b,c,b,c] will product the default binning of {'a': 'a', 'b':'b', 'c':'c'}

		Parameter
		------
		series: pd.Series, data in which the unique values will be converted to the binning dictionary
		"""
		binning_data = {}
		for val in series.unique():
			binning_data[str(val)] = str(val)
		return binning_data

	@staticmethod
	def calculate_iv(pivot_table, iv='IV'):
		"""
		Calculates information value given a pivot table with iv value in the column 'IV'

		Parameter
		------
		pivot_table: pd.DataFrame, table with the information value column
		iv: str, If other column names are used for iv
		"""
		iv = pivot_table[(pivot_table.IV.notnull()) & (pivot_table.IV != np.inf) & (pivot_table.IV  != -np.inf)]['IV'].sum()
		return iv

	@staticmethod
	def display_graph(pivot_table):
		"""
		Displays WOE values for each binning in the pivot table, for the purpose of faster recognition of binning appropriateness.

		Parameter
		------
		pivot_table: pd.DataFrame, table with the information value column
		"""
		x_labels = pivot_table.index
		x_range = list(range(len(x_labels)))
		ax = plt
		ax.bar(x_range, pivot_table['WOE'])
		plt.xticks(x_range, x_labels, rotation=90)
		plt.xlabel('bins')

	@staticmethod
	def upload_meta_data_to_gcp(meta_data, model_name=None, features=None, verbose=True):
		if model_name is None:
			error_msg = """
			Model name must be defined. Please follow the following convention.

            scoring_criteria<PRODUCT_TYPE(_cli/_cash_loan/[blank if general])>
            <REPEAT_TYPE(_new/_repeat_order/[blank is N/A])>
            <CUSTOMER_SEGMENT(_bank_mutation/_bpjs/_payslip/[blank for general])>
            <PARTNER(_blibli/_tiket/_linkaja/[blank for general])>
            <PROTOTYPE_STATUS(_prototype/[blank for production])><EXPERIMENT(_experiment_<NAME>/[blank if N/A])>
            _<DATE(YYYY-MM-DD)>
		   
			Examples:
            scoring_criteria_2020-05-05
            scoring_criteria_cli_2020-05-05
            scoring_criteria_cli_blibli_2020-05-05
            scoring_criteria_cli_blibli_experiment_josephine_2020-05-05

			"""
			raise ValueError(error_msg)
		
		binning_mapping = pd.DataFrame()

		if features is None:
			features = meta_data.keys()

		for i in features:
			bins = pd.DataFrame()

			if meta_data[i]['data_type'] == 'numerical':

				if type(meta_data[i]['binning_data']) == str:
					meta_data[i]['binning_data'] = json.loads(meta_data[i]['binning_data'])

				bins['min'] = meta_data[i]['binning_data'].copy()[:-1]
				bins['max'] = meta_data[i]['binning_data'].copy()[1:]
				bins['bin_name'] = "(" + bins['min'].astype(str).str.cat(bins['max'].astype(str), sep = ', ') + "]"
				bins['category'] = "NUMERIC_LIMIT"
			else:
				bins['value'] = meta_data[i]['binning_data'].keys()
				bins['bin_name'] = meta_data[i]['binning_data'].values()
				bins['category'] = 'STRING_EQUAL'

			true_bin = pd.DataFrame(bins.iloc[0]).transpose()[['category']]
			true_bin['default_value'] = True
			true_bin['value'] = str(meta_data[i]['default_null'])
			true_bin['bin_name'] = str(meta_data[i]['default_bin'])
			bins['default_value'] = False
			bins = pd.concat([bins, true_bin], ignore_index=True, sort=False)
			bins['modelname'] = model_name
			bins['feature_slug_name'] = i
			binning_mapping = pd.concat([binning_mapping, bins], ignore_index=True, sort=False)

		if verbose == True:
			display(binning_mapping)

		binning_mapping['createdAt'] = pd.to_datetime(datetime.datetime.now(),utc=True)
		binning_mapping['createdAt'] = binning_mapping['createdAt'].astype(str)

		# Upload to bigquery
		client = bigquery.Client(project="athena-179008")
		table_id = 'vayu_data_mart.binning_mapping'
		job_config = bigquery.LoadJobConfig(schema=[],
		write_disposition="WRITE_APPEND",)
		start = time.time()

		job = client.load_table_from_dataframe(
			binning_mapping, table_id
			, job_config=job_config
		)

		# Wait for the load job to complete.
		job.result()
		end = time.time()
		print("upload completed to vayu_data_mart.binning_mapping in {} seconds".format(end-start))
		return binning_mapping

	def upload_to_gcp(self, model_name=None, features=None, verbose=True):
		return BinningBuilder.upload_meta_data_to_gcp(self.meta_data, model_name, features, verbose)

	@staticmethod
	def load_meta_data_from_gcp(model_name=None, exact_match=False):
		client = bigquery.Client(project="athena-179008")
		model_name_query = "LIKE '%" + model_name + "%'"
		if exact_match:
			model_name_query = "= '" + model_name + "'"
		sql = """
		WITH model_binning AS (SELECT * FROM vayu_data_mart.binning_mapping WHERE
		modelname %s
		)

		SELECT * FROM model_binning WHERE
		createdAt = (SELECT max(createdAt) FROM model_binning)
			"""%model_name_query
		binning = client.query(sql).to_dataframe()
		
		binnings = binning.to_dict(orient='row')
		meta_data = {}

		for row in binnings:
			key = row['feature_slug_name']

			if key not in meta_data:
				meta_data[key] = {}
				meta_data[key]['data_type'] = 'categorical' if row['category'] == 'STRING_EQUAL' else 'numerical'

				if meta_data[key]['data_type'] == 'categorical':
					meta_data[key]['binning_data'] = {}
				else:
					meta_data[key]['binning_data'] = []

				meta_data[key]['default_bin'] = 'UNBINNED'
				meta_data[key]['default_null'] = -999999999

			if meta_data[key]['data_type'] == 'categorical':
				meta_data[key]['binning_data'][row['value']] = row['bin_name']
			elif meta_data[key]['data_type'] == 'numerical':
				meta_data[key]['binning_data'].append(row['min'])
				meta_data[key]['binning_data'].append(row['max'])

			if row['default_value']:
				if row['value'] is not None:
					meta_data[key]['default_null'] = row['value']
				meta_data[key]['default_bin'] = row['bin_name']

		for key in meta_data:
			md = meta_data[key]
			if md['data_type'] == 'numerical':
				md['binning_data'] = sorted([x for x in sorted(list(set(md['binning_data']))) if str(x) != "nan"])
				md['default_null'] = float(md['default_null'])

		return meta_data

	def get_meta_data_from_gcp(self, model_name=None):
		meta_data = BinningBuilder.load_meta_data_from_gcp(model_name)
		self.meta_data = meta_data

	def reset(self, cols=None):
		"""
		Resets data to raw_data, this means: New columns added and null handlers will be back to normal

		Parameter
		------
		pivot_table: pd.DataFrame, table with the information value column
		cols: list, If defined, the columns that are reverted to its raw data will be limited to the values in the list
		"""
		if cols is None:
			self.data = self.raw_data.copy()
		else:
			for col in cols:
				self.data[col] = self.raw_data[col]

	def add_column(self, col_name, data):
		"""
		Adds a new column to the data set and performs intime oot split so that train and test is consistent.

		Parameter
		------
		col_name: Str, name of column 
		data: pd.Series, Series for the column
		"""
		self.data[col_name] = data
		self.intime_oot_split()

	def add_column_by_merge(self, data, cols=None, left_on=None, right_on='orderId', suffix='_y', fillna=None):
		"""
		Merges new columns from data to the current data set and performs intime oot split so that train and test is consistent.

		Parameter
		------
		data: pd.Series, Series for the column
		cols: columns to be merged into the current data
		key: key from the new data frame to be mapped into current data
		"""
		if left_on is None:
			left_on = self.key
		if cols is not None:
			data = data[cols + [right_on]]
		temp = pd.merge(self.data, data, left_on=left_on, right_on=right_on, how='left', suffixes=('', suffix))
		self.data = temp
		if fillna is not None:
			for col in data.columns:
				if col != left_on and col != right_on:
					self.data[col] = self.data[col].fillna(fillna)
		self.intime_oot_split()

	@staticmethod
	def load_meta_data_from_file(path):
		"""
		Gets meta data from a file. This means, information on binning and null handling will be interpreted from the
		supplied meta data during transformation.

		Parameter
		------
		data: pd.Series, Series for the column
		cols: columns to be merged into the current data
		key: key from the new data frame to be mapped into current data
		"""
		meta_data = pd.read_csv(path, index_col=0).to_dict()
		for key in meta_data:
			meta_data[key]['binning_data'] = json.loads(meta_data[key]['binning_data'])
			if meta_data[key]['data_type'] == 'numerical':
				meta_data[key]['default_null'] = int(meta_data[key]['default_null'])
			if type(meta_data[key]['binning_data']) == str:
				meta_data[key]['binning_data'] = json.loads(meta_data[key]['binning_data'])
		return meta_data

	def get_meta_data_from_file(self, path):
		"""
		Gets meta data from a file. This means, information on binning and null handling will be interpreted from the
		supplied meta data during transformation.

		Parameter
		------
		data: pd.Series, Series for the column
		cols: columns to be merged into the current data
		key: key from the new data frame to be mapped into current data
		"""
		meta_data = BinningBuilder.load_meta_data_from_file(path)
		for key in meta_data:
			self.meta_data[key] = meta_data[key]
		
	def save_meta_data_to_file(self, path):
		to_save = self.meta_data.copy()
		for key in to_save:
			binning_data = to_save[key]['binning_data']
			to_save[key]['binning_data'] = json.dumps(binning_data)
		meta_data = pd.DataFrame(to_save)
		meta_data.to_csv(path)
		
	def get_meta_data_from_dict(self, meta_data):
		for key in meta_data:
			self.meta_data[key] = meta_data[key]
		
	def save_meta_data_to_dict(self):
		return self.meta_data

	def get_binning(raw_values, meta_data_):
		# handle null 
		raw_values = raw_values.fillna(meta_data_['default_null'])

		# binning mapping
		binning_data = meta_data_['binning_data']
		categorical = meta_data_['data_type'] == 'categorical'
		if categorical:
			bin_values = raw_values.astype(str).apply(lambda x: binning_data[x] if x in binning_data else np.nan)
		else:
			bin_values = pd.cut(raw_values, binning_data) 
		if categorical:
			# handle default category
			bin_values = bin_values.fillna(meta_data_['default_bin'])
		else:
			# handle default bin
			default_bin = meta_data_['default_bin']
			bin_values = bin_values.cat.add_categories(default_bin).fillna(default_bin)
		return bin_values

	@staticmethod
	def auto_binning(series):
		data_type = series.dtype
		categorical = (data_type != 'int64' and data_type != 'float64')
		meta_data = {}
		meta_data['default_bin'] = 'UNBINNED'
		meta_data['default_null'] = -9999999999
		meta_data['data_type'] = "categorical" if categorical else "numerical" 
		series = series.fillna(meta_data['default_null'])
		binning_data = None
		if categorical:
			binning_data = BinningBuilder.get_default_categorical_binning(series)
		else:
			binning_data = list(series.quantile(np.arange(0, 1, .1)).unique()) + [np.inf]
			binning_data = [binning_data[0]-1] + binning_data
		meta_data['binning_data'] = binning_data
		return meta_data
		
		
	def calculate_ivs(self, cols):
		ivs = []
		for col in cols:
			auto_binning = col not in self.meta_data
			pivot = self.transform_woe_and_pivot(col, auto_binning=auto_binning, display_graph=False)
			iv = BinningBuilder.calculate_iv(pivot)
			ivs.append([col, iv])
		return sorted(ivs, key=lambda x: x[1], reverse=True)
		
	def transform_woe_and_pivot(self, col, binning_data=None, display_graph=True, by_month=False,
			 categorical=False, auto_binning=False, default_bin=None, default_null=None,
			print_iv=False, by_segment=None, filter_column=None):
		"""
			

		Parameter
		------
		categorical: Bool,  When True, coerces feature to be treated as categorical. Else, the type will be inferred from data type.
		auto_binning: Bool, When True uses deciles for numerical features and 1-to-1 mappping for categorical. 
		
		"""
		# coerce categorical when categorical = True
		data_type = self.train[col].dtype 
		if categorical == False:
			if data_type != 'int64' and data_type != 'float64': 
				categorical = True
		if binning_data is None and col not in self.meta_data and auto_binning == False:
			raise ValueError("Binning data has to be defined for categorical AND numerical data. List for numerical, dictionary for categorical.")	
		# determine binning
		if auto_binning:
			# TODO: if null values are present, default null value should exist in data.
			self.meta_data[col] = BinningBuilder.auto_binning(self.train[col])
		else:
			if col not in self.meta_data:
				self.meta_data[col] = {}

			if default_null is None and 'default_null' not in self.meta_data[col]:
				self.meta_data[col]['default_null'] = -9999999999 
			elif default_null is not None:
				self.meta_data[col]['default_null'] = default_null

			if default_bin is None and 'default_bin' not in self.meta_data[col]:
				self.meta_data[col]['default_bin'] = "UNBINNED"
			elif default_bin is not None:
				self.meta_data[col]['default_bin'] = default_bin

			self.meta_data[col]['data_type'] = "categorical" if categorical else "numerical" 
			if binning_data is not None:
				self.meta_data[col]['binning_data'] = binning_data

		# save state
		self.train_binning[col] = BinningBuilder.get_binning(self.train[col], self.meta_data[col])
		if by_segment is not None:
			self.train_binning[by_segment] = self.train[by_segment]

		data = self.train_binning
		if filter_column is not None:
			data = self.train_binning[self.train[filter_column]]
		pivot_table = BinningBuilder.calculate_woe(data, col, self.flag, key=self.key, by_month=by_month,
        by_segment=by_segment)
		if print_iv:
			print("IV: %2f" % BinningBuilder.calculate_iv(pivot_table))
		if not by_month and display_graph:
			BinningBuilder.display_graph(pivot_table)
		return pivot_table
	
	@staticmethod
	def calculate_woe(data, index_column, target_column, key='orderId', by_month=False, month_column="month",
                    by_segment=None):
		pivot_table = pd.pivot_table(data, index=[index_column], columns=[target_column], values=key, aggfunc='count')
		if by_month:
			pivot_table = pd.pivot_table(data, index=[month_column, index_column], columns=[target_column], values=key, aggfunc='count')
		if by_segment:
			pivot_table = pd.pivot_table(data, index=[by_segment, index_column], columns=[target_column], values=key, aggfunc='count')
		if by_segment and by_month:
			pivot_table = pd.pivot_table(data, index=[month_column, by_segment, index_column], columns=[target_column], values=key, aggfunc='count')

		pivot_table = pivot_table.fillna(0)
		pivot_table["TOTAL"] = pivot_table[1]+pivot_table[0]
		pivot_table['BADRATE'] = pivot_table[1]/pivot_table["TOTAL"]
		pivot_table['DIST_BAD'] = pivot_table[1]/pivot_table[1].sum()
		pivot_table['DIST_GOOD'] = pivot_table[0]/pivot_table[0].sum()
		pivot_table['WOE'] = np.log(pivot_table['DIST_BAD']/pivot_table['DIST_GOOD'])
		pivot_table['IV'] = pivot_table['WOE'] * (pivot_table['DIST_BAD'] - pivot_table['DIST_GOOD'])
		return pivot_table

	def multi_pivot(self, cols, by_month=False):
		if by_month:
			cols = ["month"] + cols
		pivot_table = pd.pivot_table(self.train_binning, index=cols, columns=[self.flag], values=self.key, aggfunc='count')
		pivot_table = pivot_table.fillna(0)
		pivot_table["TOTAL"] = pivot_table[1]+pivot_table[0]
		pivot_table['BADRATE'] = pivot_table[1]/pivot_table["TOTAL"]
		pivot_table['DIST_BAD'] = pivot_table[1]/pivot_table[1].sum()
		pivot_table['DIST_GOOD'] = pivot_table[0]/pivot_table[0].sum()
		pivot_table['WOE'] = np.log(pivot_table['DIST_BAD']/pivot_table['DIST_GOOD'])
		pivot_table['IV'] = pivot_table['WOE'] * (pivot_table['DIST_BAD'] - pivot_table['DIST_GOOD'])
		return pivot_table 

	@staticmethod
	def generate_woe_mapper(data, index_column, target_column, key='orderId'):
		pivot_table = BinningBuilder.calculate_woe(data, index_column, target_column, key)
		return pivot_table[['WOE']].to_dict()['WOE']
		
	def intime_oot_split(self):
		self.train = self.data[(self.data[self.date_column] >= self.start_dateintime) & (self.data[self.date_column] <= self.end_dateintime)] 
		self.train = self.train.reset_index(drop=True)
		self.test = self.data[(self.data[self.date_column] >= self.start_date_oot) & (self.data[self.date_column] <= self.end_date_oot)] 
		self.test = self.test.reset_index(drop=True)
		if self.train_binning is None:
			self.train_binning = self.train[[self.key, self.flag, "month"]].copy()

	def _generate_pivot_table_for_graphing(self, col):
		if col not in self.meta_data or self.meta_data[col]['binning_data'] is None:
			raise ValueError("Binning data must be defined prior to graphing")
		by_month = self.transform_woe_and_pivot(col, by_month=True)
		by_month = by_month.unstack(col)
		by_month['TOTAL'] = by_month['TOTAL'].fillna(0)
		by_month = by_month.stack(col)
		by_month["DIST"] = by_month["TOTAL"].divide(by_month.groupby(level=0)["TOTAL"].sum())
		return by_month

	def generate_kd_plot(self, col, data=None):
		"""
		Displays kernel density plot to visualize difference in distribution between 0 and 1 

		Parameter
		------
		pivot_table: pd.DataFrame, table with the information value column
		"""
		if data is None:
			data = self.train

		bad_data = data[data[self.flag] == 1]
		good_data = data[data[self.flag] == 0]
		
		sns.kdeplot(data=bad_data[col], label="bad", color="red")
		sns.kdeplot(data=good_data[col], label="good", color="blue")

	def generate_box_and_whiskers(self, col, mode="badrate"):
		"""
		Generate box and whiskers plot to visualize mean, median, and upper/lower boundaries 
		for distribution and badrate.

		Parameter
		------
		col: str, column to plot box and whiskers
		mode: str, option to display changes in 'distribution' or 'badrate'
		"""
		by_month = self._generate_pivot_table_for_graphing(col)
		by_month = by_month.reset_index()
		distributions = []
		labels =  by_month[col].unique()
		col_mode = "BADRATE"
		if mode == "distribution":
			col_mode = "DIST"
		for col_value in labels:
			distribution = by_month[by_month[col]== col_value][col_mode]
			distributions.append(distribution)
		plt.figure(figsize=(10,5))
		plt.boxplot(distributions, labels=labels.astype(str));
		plt.xticks(rotation=30)
		plt.ylabel(col_mode)
		plt.xlabel("Binnings")


	def generate_animated_bar_chart(self, col, mode="badrate"):
		by_month = self._generate_pivot_table_for_graphing(col)
		by_month = by_month.reset_index()
		by_month[col] = by_month[col].astype(str)
		if mode == "distribution":
			color_col = "BADRATE"
			y_col = "DIST"
			y_max = by_month[y_col].max()
			fig = px.bar(by_month, 
					x=col, y=y_col,
					color=color_col, 
					color_continuous_scale=["green", "red"],
					range_color=[0.10, 0.3],
					animation_frame="month", range_y=[0,y_max])
		else:
			color_col = "DIST"
			y_col = "BADRATE"
			y_max = by_month[y_col].max()
			fig = px.bar(by_month,
					x=col, y=y_col,
					color=color_col,
					color_continuous_scale=["blue", "black"],
					range_color=[0, 0.3],
					animation_frame="month",
					range_y=[0,y_max])
		fig.show()

