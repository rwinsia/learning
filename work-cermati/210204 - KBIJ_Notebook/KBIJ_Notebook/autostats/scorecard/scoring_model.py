import pandas as pd
import numpy as np
import json
import matplotlib.pyplot as plt
from google.cloud import bigquery
from scorecard.binning_builder import BinningBuilder


class ModelData(dict):
    def __getattr__(self, name):
        return self[name]

class ScoringModel(object):
    """
    ScoringModel(filename)

    A class used for creating score cards mainly used for Indodana's credit scoring.

    Parameter
    ---------
    filename : str
    Path to json file if taking previous json model.
     Leave None if creating a ScoringModel from scratch.
    """
    def __init__(self, filename=None):
        json_model = { "rules": {}}

        if filename is not None:
            with open(filename) as file:
                json_model = json.loads(file.read())

        self.original_model = ModelData(json_model)
        self.model = ModelData(json_model.copy())
        self.validate_json()
        self.meta_data = self.json_to_metadata()

    def save_model(self, filename):
        self.validate_json()
        with open(filename, "w") as file:
            json.dump(self.model, file)

    def print_feature_names(self, rule_name=None):
        if rule_name is None:
            for rule in self.model.rules:
                print("\n" + rule + ":")
                for feature in self.model.rules[rule]:
                    print(feature)
        else:
            for feature in self.model.rules[rule_name]:
                print(feature)

    def print_feature(self, feature):
        rule = self._fetch_rule(feature)

        if rule is None:
            raise ValueError("Feature %s does not exist in the current model."%feature)
        
        feature_items = self.model.rules[rule][feature]
        for i, score_item in enumerate(feature_items):
            print("Index: %d"%i)
            print(score_item)
        
        print("Printed a total of %d score items"%len(feature_items))

    def get_rule_list(self):
        rule_list = []
        for rule in self.model.rules:
            rule_list.append(rule)
        return rule_list

    def get_feature_list(self, rule_name=None):
        if rule_name is not None:
            return list(self.model.rules[rule_name].keys())
        feature_list = []
        for rule in self.model.rules:
            for feature in self.model.rules[rule]:
                feature_list.append(feature)
        return feature_list

    def rename_rule(self, old_rule=None, new_rule=None):
        if old_rule not in self.model.rules:
            raise ValueError("Rule %s does not exist in the current model."%old_rule)

        self.model.rules[new_rule] = self.model.rules[old_rule].copy()
        del self.model.rules[old_rule]

    def add_rule(self, rule_name):
        if rule_name in self.model.rules:
            raise ValueError("Rule %s already exists in current model."%rule_name)
        self.model.rules[rule_name] = {}

    def rename_feature(self, old_feature=None, new_feature=None):
        renamed = False
        rule = self._fetch_rule(old_feature)

        if rule is None:
            raise ValueError("Feature %s does not exist in the current model."%old_feature)

        self.model.rules[rule][new_feature] = self.model.rules[rule][old_feature].copy()
        del self.model.rules[rule][old_feature]

    def append(self, feature=None, score_item=None):
        if type(score_item) is not dict:
            raise ValueError("Score item parameter is not dictionary.")

        rule = self._fetch_rule(feature)
        self.model.rules[rule][feature].append(score_item)

    def _fetch_rule(self, feature):
        for rule in self.model.rules:
            if feature in self.model.rules[rule]:
                return rule
        return None

    @staticmethod
    def _sanitize_score(number):
        return round(number)

    @staticmethod
    def _sanitize_number(number):
        if number == np.inf:
            return "inf"
        if number == -np.inf:
            return "-inf"
        return number

    @staticmethod
    def _create_numeric_item(score, min_value, max_value, default):
        score_item = {}
        score_item["min"] = ScoringModel._sanitize_number(min_value)
        score_item["max"] = ScoringModel._sanitize_number(max_value)
        score_item["score"] = ScoringModel._sanitize_score(score)

        if default:
            score_item["default"] = True

        return score_item

    @staticmethod
    def _create_string_equal_item(score, strings, default):
        score_item = {}
        score_item["score"] = ScoringModel._sanitize_score(score)
        score_item["in"] = strings

        if default:
            score_item["default"] = True
        
        return score_item
    
    def add_or_replace_feature(self, rule=None, feature=None, score_items=None):
        if rule not in self.model.rules:
            self.model.rules = {}

        if type(score_items) is not list:
            raise ValueError("Score items parameter must be a list.")

        self.model.rules[rule][feature] = score_items

    def append_string_equal(self, feature=None, score=None, strings=None, default=False):
        score_item = ScoringModel._create_string_equal_item(score, strings, default)
        self.append(feature, score_item)

    def append_numeric_limit(self, feature=None, score=None, min_value=None, max_value=None,
     default=None):
        score_item = ScoringModel._create_numeric_item(score, min_value, max_value, default)
        self.append(feature, score_item)

    def edit_feature_by_index(self, feature=None, index=None, score_item=None):
        rule = self._fetch_rule(feature)
        self.model.rules[rule][feature][index] = score_item
    
    def edit_numeric_feature_by_index(self, feature=None, index=None, score=None,
     min_value=None, max_value=None, default=None):
        rule = self._fetch_rule(feature)

        if rule is None:
            raise ValueError("Feature %s does not exist in the current model."%feature)
    
        score_item = self.model.rules[rule][feature][index]

        if score is not None:
            score_item["score"] = score
        if min_value is not None:
            score_item["min"] = min_value
        if max_value is not None:
            score_item["max"] = max_value
        if default is not None:
            score_item["default"] = default

        self.edit_feature_by_index(feature, index, score_item)
    
    def edit_string_equal_feature_by_index(self, feature=None, index=None, score=None, strings=None, default=None):
        rule = self._fetch_rule(feature)

        if rule is None:
            raise ValueError("Feature %s does not exist in the current model."%feature)
        score_item = self.model.rules[rule][feature][index]
        
        if score is not None:
            score_item["score"] = score
        if strings is not None:
            score_item["in"] = strings
        if default is not None:
            score_item["default"] = default

        self.edit_feature_by_index(feature, index, score_item)

    def remove_feature(self, feature=None):
        rule = self._fetch_rule(feature)
        del self.model.rules[rule][feature]

    def remove_score_item_by_index(self, feature=None, index=None):
        rule = self._fetch_rule(feature)
        del self.model.rules[rule][feature][index]

    def reset(self):
        self.model = ModelData(self.original_model.copy())
    
    def __is_in_categorical_binning(score_item, value):
        value = str(value)
        if type(score_item["in"]) == str:
            if value == score_item["in"]:
                return True 
        else:
            if value in score_item["in"]:
                return True 
        return False
    
    def __is_in_numerical_binning(score_item, value):
        min_value = score_item["min"]
        max_value = score_item["max"]
        if min_value == "-inf":
            min_value = -np.inf
        else:
            min_value = float(min_value)
        if max_value == "inf":
            max_value = np.inf
        else:
            max_value = float(max_value)
        value = float(value)
        if value >= min_value and value <= max_value:
            return True
        return False

    def get_feature_score(self, feature, value, verbose=True):
        rule = self._fetch_rule(feature)
        feature_items = self.model.rules[rule][feature]
        default_value = None
        try: 
            for score_item in feature_items:
                score = score_item["score"]
                if "default" in score_item:
                    default_value = score
                if value is not None:
                    if "in" in score_item:
                        if ScoringModel.__is_in_categorical_binning(score_item, value): 
                            return score
                    elif "min" in score_item:
                        if ScoringModel.__is_in_numerical_binning(score_item, value): 
                            return score
        except:
            for score_item in feature_items:
                if "default" in score_item:
                    return score_item["score"]

        if default_value is None:
            if verbose:
                print("WARNING %s doesn't have a default score"%feature)
            default_value = 0
        return default_value
    
    def __score(self, data, base_score=None, verbose=True):
        if base_score is None:
            raise ValueError("Base score must be defined for scoring.")
        for rule in self.model.rules:
            for feature in self.model.rules[rule]:
                score = self.get_feature_score(feature, data[feature], verbose=verbose)
                base_score = base_score + score
        return base_score

    def score(self, data, base_score=None, verbose=True):
        if base_score is None:
            raise ValueError("Base score must be defined for scoring.")
        return data.apply(lambda row: self.__score(row, base_score=base_score, verbose=verbose), axis=1)

    @staticmethod
    def get_rule_slug_name_bigquery():
        client = bigquery.Client(project="athena-179008")
        query_job = client.query(
            """
            SELECT distinct rule_slug_name 
            FROM data-platform-indodana.vayu.indodana_featurestores_features
            """
            )
        df = query_job.to_dataframe()
        list = df['rule_slug_name'].values.tolist()
        return list

    @staticmethod
    def get_feature_slug_name_bigquery():
        client = bigquery.Client(project="athena-179008")
        query_job = client.query(
            """
            SELECT distinct feature_slug_name 
            FROM data-platform-indodana.vayu.indodana_featurestores_features
            """
            )
        df = query_job.to_dataframe()
        list = df['feature_slug_name'].values.tolist()
        return list

    def validate_json(self):
        prod_rule_slug_name_list = ScoringModel.get_rule_slug_name_bigquery()
        prod_feature_slug_name_list = ScoringModel.get_feature_slug_name_bigquery()
        json_rule_slug_name_list = ScoringModel.get_rule_list(self)
        json_feature_slug_name_list = ScoringModel.get_feature_list(self)
        print('Checking for rule slug name\n')
        for rule in json_rule_slug_name_list:
            if rule not in prod_rule_slug_name_list:
                print(rule, 'not in production')
        print('\nChecking for feature slug name\n')
        for feature in json_feature_slug_name_list:
            if feature not in prod_feature_slug_name_list:
                print(feature, 'not in production')
        print('\nDone')


    def json_to_metadata(self):
        model_meta_data = {}

        for rule in self.model["rules"]:
            for feat in self.model["rules"][rule]:
                model_meta_data[feat] = {}
                model_meta_data[feat]["default_null"] = -999999999
                
                binnings = self.model["rules"][rule][feat]
                if any("in" in binning for binning in binnings):
                    model_meta_data[feat]["data_type"] = "categorical"
                else:
                    model_meta_data[feat]["data_type"] = "numerical"
                
                if model_meta_data[feat]["data_type"] == "categorical":
                    model_meta_data[feat]["default_bin"] =  "UNBINNED"
                else:
                    model_meta_data[feat]["default_bin"] = -999999999

                model_meta_data[feat]["binning_data"] = set([])
                for binning in binnings:
                    
                    if "default" in binning.keys():
                        if binning["default"]:
                            if model_meta_data[feat]["data_type"] == "categorical":
                                model_meta_data[feat]["default_bin"] = binning["in"][0]
                            else:
                                if binning['min'] != '-inf':
                                    model_meta_data[feat]["default_bin"] = binning["max"]
                    
                    if model_meta_data[feat]["data_type"] == "categorical":
                        model_meta_data[feat]["binning_data"].update(binning['in'])
                    else:
                        model_meta_data[feat]["binning_data"].add(float(binning["min"]))
                        model_meta_data[feat]["binning_data"].add(float(binning["max"]))
                
                if model_meta_data[feat]["data_type"] == "numerical":
                    model_meta_data[feat]["binning_data"] = sorted(list(model_meta_data[feat]["binning_data"]))
                else:
                    model_meta_data[feat]["binning_data"] = {binning:binning for binning in model_meta_data[feat]["binning_data"]} 
        return model_meta_data
    
    def upload_meta_data_to_gcp(self, model_name=None, features=None, verbose=True):
        return BinningBuilder.upload_meta_data_to_gcp(self.meta_data, model_name, features, verbose)
