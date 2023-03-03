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

class ConvertSteepbankXYToLonLatTests(unittest.TestCase):

    def test_convert_steepbank_xy_to_lon_lat(self):
        x = 148575.75
        y  = 254252.32
        xy_converted_lon_lat = convert_steepbank_xy_to_lon_lat(x, y)

        expected_longitude = -111.4206354
        expected_latitude = 57.0059799

        self.assertAlmostEqual(xy_converted_lon_lat['longitude'], expected_longitude, delta = 0.0001)
        self.assertAlmostEqual(xy_converted_lon_lat['latitude'], expected_latitude, delta = 0.0001)

class ConvertForthillsXYToLonLatTests(unittest.TestCase):

    def test_convert_forthills_xy_to_lon_lat(self):
        x = 465807
        y  = 6359454
        xy_converted_lon_lat = convert_forthills_xy_to_lon_lat(x, y)

        expected_longitude = -111.568650963607
        expected_latitude = 57.3766283928482

        self.assertAlmostEqual(xy_converted_lon_lat['longitude'], expected_longitude, delta = 0.0001)
        self.assertAlmostEqual(xy_converted_lon_lat['latitude'], expected_latitude, delta = 0.0001)      
