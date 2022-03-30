# -*- encoding: UTF-8 -*-
import yaml
import os
import pandas as pd


def init():
    global config
    root_dir = os.path.dirname(os.path.abspath(__file__))  # This is your Project Root
    config_file = os.path.join(root_dir, 'config.yaml')
    pd.set_option('display.max_columns', 20)
    pd.set_option('display.max_row', 500)
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)