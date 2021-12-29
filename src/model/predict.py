from model.assets import KaladinData
from model.shap_values import train_shap_explainer, get_shap_explanations, get_top_explanation_urls
from tensorflow.keras.models import load_model
import numpy as np
import pandas as pd
from common.utils import pickle_me

def predict_all(tc_list, days_list, use_eval, explainers=None, data_dct=None):
    """
    tc_list : List of integers specifying time controls to use
    days_list : List of integers specifying time periods to use
    data_dct : None for training, live_data from prepare_data.py when live
    """
    predictions = []
    for tc in tc_list:
        for days in days_list:
            if (tc, days) not in data_dct:
                continue
            data=KaladinData(days, tc, use_eval, data_dct)
            if explainers is not None:
                explainer = explainers[(use_eval, tc, days)]
            else:
                explainer = None
            predictions.append(
                make_predictions(days, tc, use_eval, 
                data.test_user_list, data.test_inputs, data.test_labels, explainer=explainer)
            )
    df = pd.concat(predictions)
    df = df.reset_index(drop=True)
    df = df.loc[df.groupby('user')['pred'].idxmax()].copy()
    
    if data_dct is None:
        pickle_me(df, 'model/output/eval{}_test_set_preds.pkl'.format(use_eval))
    
    return df

def make_predictions(days, tc, use_eval, users, inputs, labels=None, explainer=None):
    directory = 'model/eval{}_tc{}_days{}/'.format(use_eval, tc, days)

    # Make model predictions
    model = load_model(directory+'model.SavedModel')
    preds = model.predict(inputs)

    # Get feature explanations
    if explainer is None:
        explainer = train_shap_explainer(directory)
    shap_values = explainer.shap_values(inputs)

    # Format explanations
    shap_df = get_shap_explanations(users, shap_values, directory)

    # Convert top relevant insights to URLs
    insight_urls = get_top_explanation_urls(shap_df, tc, days, top_n=3)

    # Combine output and write to disk
    output = pd.DataFrame({
        'user':users,
        'tc':tc,
        'days':days,
        'label':labels,
        'pred':np.ravel(preds)
    })
    output = output.merge(insight_urls, how='left')
    return output

if __name__ == '__main__':
    predict_all([2, 6], [180,], use_eval=0, explainer=None, data_dct=None)