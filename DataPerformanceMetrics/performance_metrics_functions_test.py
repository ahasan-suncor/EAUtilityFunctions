# Databricks notebook source
import unittest

class CalculateClassificationMetricsTests(unittest.TestCase):
    
    def setUp(self):
        self.pandas_df_test = pd.DataFrame({
            'actual': [0, 1, 1, 0, 1, 0, 2, 0, 1, 1, 2, 2, 2, 0, 0, 1, 0, 1, 2, 0],
            'ypred':  [2, 1, 0, 0, 1, 0, 2, 0, 0, 1, 2, 2, 2, 1, 0, 1, 1, 0, 2, 0]
        })
        
    def test_calculate_classification_metrics_macro(self):
        expected_metrics = {'f1_score': 0.72
                          , 'precision': 0.71
                          , 'recall': 0.73
        }
        actual_metrics = calculate_classification_metrics(self.pandas_df_test, 'macro')
        self.assertAlmostEqual(actual_metrics['f1_score'], expected_metrics['f1_score'], delta = 0.01)
        self.assertAlmostEqual(actual_metrics['precision'], expected_metrics['precision'], delta = 0.01)
        self.assertAlmostEqual(actual_metrics['recall'], expected_metrics['recall'], delta = 0.01)
        
class CalculateRegressionMetricsTests(unittest.TestCase):
    
    def setUp(self):
        self.pandas_df_test = pd.DataFrame({
        'ypred': [-11.689722554411759, -28.128167134326144, 77.22591946066083, -66.44019822146333, 125.41659207984007,
                  120.60580021175078, 99.6612493020604, -58.864176708031785, -73.36429475640857, -4.527599841170006,
                  -107.7024821657279, -30.388992502972375, -16.12767075536393, 6.991598736680632, 5.138766035409414,
                  -98.9878586131691, -68.02359754437784, 14.846005955874492, 53.596320971485056, -125.87107802692269],
        'actual': [14.735059542462793, -40.678294731144454, 206.61159819795736, -148.35609491369252, 225.78041056157235,
                   278.88233991066085, 129.7911607952765, -61.17480025457889, -95.22618677777012, 47.23014523667997,
                   -199.32671872818392, -20.179457739964676, 30.52535579180926, -25.565390216421008, 81.35797735068243,
                   -209.42324710211057, -91.03627485465356, 11.319279390217456, 172.74826654378631, -212.6676385822535]
        })
        
    def test_calculate_regression_metrics(self):
        expected_metrics = {'MAPE': 0.64
                          , 'MAE': 60.76
                          , 'RMSE': 5775.74
                           }
        actual_metrics = calculate_regression_metrics(self.pandas_df_test)
        self.assertAlmostEqual(actual_metrics['MAPE'], expected_metrics['MAPE'], delta = 0.01)
        self.assertAlmostEqual(actual_metrics['MAE'], expected_metrics['MAE'], delta = 0.01)
        self.assertAlmostEqual(actual_metrics['RMSE'], expected_metrics['RMSE'], delta = 0.01)
