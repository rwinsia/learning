import os
import pandas as pd
import numpy as np
import pickle
from fuzzywuzzy import fuzz
import Levenshtein as lev

def get_fuzz(x, y):
    similarity = max(fuzz.token_set_ratio(x, y),fuzz.token_set_ratio(y, x))
    return similarity

def get_distance(x, y):
    dist = lev.distance(x, y)
    return dist

def get_distance_left(x, y):
    x_dense = x.replace(" ", "")
    y_dense = y.replace(" ", "")
    min_length = min(len(x_dense), len(y_dense))
    x_dense_left = x_dense[:min_length]
    y_dense_left = y_dense[:min_length]
    return lev.distance(x_dense_left, y_dense_left)

def get_distance_right(x, y):
    x_dense = x.replace(" ", "")
    y_dense = y.replace(" ", "")
    min_length = min(len(x_dense), len(y_dense))
    x_dense_right = x_dense[min_length*-1:]
    y_dense_right = y_dense[min_length*-1:]
    return lev.distance(x_dense_right, y_dense_right)

def name_match(x, y, fuzz_threshold=65, distance_threshold=4,
             dense_length_threshold=5, dense_distance_threshold=2):
    if x is None or y is None or x is np.nan or y is np.nan:
        return -999
    if get_fuzz(x, y) >= fuzz_threshold:
        return 1
    if get_distance(x, y) <= distance_threshold:
        return 1
    
    
    x_dense = x.replace(" ", "")
    y_dense = y.replace(" ", "")
    min_length = min(len(x_dense), len(y_dense))

    if min_length >= dense_length_threshold:
        if get_distance_right(x,y) <= dense_distance_threshold or get_distance_left(x,y) <= dense_distance_threshold:
            return 1
    
    return 0


def load_transformers(prod_path, res_path=None, transformers={}):
    for filename in os.listdir(prod_path):
        if filename.endswith('.pickle'):
            with open(prod_path+filename, "rb") as f:
                transformers[filename.split('.')[0]] = pickle.load(f)
    if res_path:
        for filename in os.listdir(res_path):
            if filename.endswith('.pickle'):
                with open(res_path+filename, "rb") as f:
                    transformers[filename.split('.')[0]] = pickle.load(f)
    return transformers

def calc_iv(df, feature, target, pr=False):
    """
    Set pr=True to enable printing of output.

    Output:
      * iv: float,
      * data: pandas.DataFrame
    """

    lst = []

    df[feature] = df[feature].fillna("NULL")

    for i in range(df[feature].nunique()):
        val = list(df[feature].unique())[i]
        lst.append([feature,                                                        # Variable
                    val,                                                            # Value
                    df[df[feature] == val].count()[feature],                        # All
                    df[(df[feature] == val) & (df[target] == 0)].count()[feature],  # Good (think: Fraud == 0)
                    df[(df[feature] == val) & (df[target] == 1)].count()[feature]]) # Bad (think: Fraud == 1)

    data = pd.DataFrame(lst, columns=['Variable', 'Value', 'All', 'Good', 'Bad'])

    data['Share'] = data['All'] / data['All'].sum()
    data['Bad Rate'] = data['Bad'] / data['All']
    data['Distribution Good'] = (data['All'] - data['Bad']) / (data['All'].sum() - data['Bad'].sum())
    data['Distribution Bad'] = data['Bad'] / data['Bad'].sum()
    data['WoE'] = np.log(data['Distribution Good'] / data['Distribution Bad'])

    data = data.replace({'WoE': {np.inf: 0, -np.inf: 0}})

    data['IV'] = data['WoE'] * (data['Distribution Good'] - data['Distribution Bad'])

    data = data.sort_values(by=['Variable', 'Value'], ascending=[True, True])
    data.index = range(len(data.index))

    if pr:
        print(data)
        print('IV = ', data['IV'].sum())


    iv = data['IV'].sum()
    # print(iv)

    return iv, data

def generate_json(variables, transformers, pivot_tables):
    for x in variables :
        x = x.replace('_WOE', '')
        json_dict = {}
        if transformers[x].data_type == 'Categorical':
            temp_pivot = pivot_tables[x]
            temp_pivot['in'] = temp_pivot[transformers[x].var_name].map(transformers[x].binning_data)
            json_dict[transformers[x].var_name] = temp_pivot[['in', 'score']].to_dict(orient='records')
        else:
            temp_pivot = pivot_tables[x]
            temp_pivot['min'] = temp_pivot[transformers[x].var_name].apply(lambda x: x.left)
            temp_pivot['max'] = temp_pivot[transformers[x].var_name].apply(lambda x: x.right)
            json_dict[transformers[x].var_name] = temp_pivot[['min', 'max', 'score']].to_dict(orient='records')

def save_to_excel(model, excel_name, verbose=False):
    with pd.ExcelWriter(excel_name) as writer:
        model.intime.trained_data.to_excel(writer, sheet_name='Train Data')
        if verbose:
            print('Train data Done')
        model.oot.trained_data.to_excel(writer, sheet_name='OOT Data')
        if verbose:
            print('OOT data Done')
        model.scorecard.to_excel(writer, sheet_name='SCORECARD')
        if verbose:
            print('Scorecard data Done')
        model.performance.to_excel(writer, sheet_name='Model Performance')
        if verbose:
            print('Performances data Done')
            print(f"Please open {excel_name}")
