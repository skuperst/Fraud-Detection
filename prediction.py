import os
import pandas as pd
import pickle
import xgboost

def prediction_func(my_dict):

    # Load the model
    xg_boost_model = xgboost.XGBClassifier()
    xg_boost_model.load_model('xgb_model.json')

    # Convert the json input into a dataframe
    df = pd.DataFrame([my_dict])

    # Load the categorical dictionary
    with open('category_dict.pkl', 'rb') as file:
        category_dict = pickle.load(file)

    # Convert object columns to categorical columns
    for col, categories in category_dict.items():
        if col in df.columns:
            # Convert the column in df to categorical using the categories from category_dict
            df[col] = pd.Categorical(df[col], categories=categories)


    return xg_boost_model.predict(df)[0]