# Databricks notebook source
import unittest

class RemoveStopwordsTests(unittest.TestCase):

    def test_remove_stopwords_sentence(self):
        input_text = 'The quick brown fox jumps over the lazy dog.'
        expected_output = 'quick brown fox jumps lazy dog.'
        self.assertEqual(remove_stopwords(input_text), expected_output)
        
    def test_remove_stopwords_single_word(self):
        input_text = 'Amazing'
        expected_output = 'Amazing'
        self.assertEqual(remove_stopwords(input_text), expected_output)
        
    def test_remove_stopwords_non_alpha_numeric(self):
        input_text = '12 # @3456 $%^ ( &* !'
        expected_output = '12 # @3456 $%^ ( &* !'
        self.assertEqual(remove_stopwords(input_text), expected_output)

    def test_remove_stopwords_empty_string(self):
        input_text = ''
        expected_output = ''
        self.assertEqual(remove_stopwords(input_text), expected_output)

    def test_remove_stopwords_stopword_only_input(self):
        input_text = 'so what the an the and a'
        expected_output = ''
        self.assertEqual(remove_stopwords(input_text), expected_output)
        
class TestLemmatizeText(unittest.TestCase):

    def test_lemmatize_text_with_punctuation(self):
        input_text = 'am, are, is!'
        expected_output = 'be be be'
        self.assertEqual(lemmatize_text(input_text), expected_output)

    def test_lemmatize_text_with_same_word(self):
        input_text = "car, cars, car's, cars'"
        expected_output = 'car car car car'
        self.assertEqual(lemmatize_text(input_text), expected_output)
        
    def test_lemmatize_text_sentence(self):
        input_text = 'The striped bats are hanging on their feet for best.'
        expected_output = 'The striped bat be hang on their foot for best'
        self.assertEqual(lemmatize_text(input_text), expected_output)
        
    def test_lemmatize_text_with_a_verb(self):
        input_text = 'organizeing,'
        expected_output = 'organize'
        self.assertEqual(lemmatize_text(input_text), expected_output)
