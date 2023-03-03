# Databricks notebook source
import unittest

class ConvertSteepbankXYToUTMTests(unittest.TestCase):

    def test_convert_steepbank_xy_to_utm(self):
        x = 148575.75
        y  = 254252.32
        xy_converted_utm = convert_steepbank_xy_to_utm(x, y)

        expected_easting = 474451.5002193068
        expected_northing = 6318130.190424226

        self.assertAlmostEqual(xy_converted_utm['easting'], expected_easting, delta = 0.0001)
        self.assertAlmostEqual(xy_converted_utm['northing'], expected_northing, delta = 0.0001)
