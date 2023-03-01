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
