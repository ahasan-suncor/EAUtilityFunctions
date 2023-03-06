# Databricks notebook source
import unittest

class TestGetShiftidFromTimestamp(unittest.TestCase):

    def test_get_shiftid_from_timestamp_day_shift(self):
        timestamp = datetime(2023, 3, 3, 8, 0, 0)
        day_shift_start_time = time(8, 0, 0)
        expected_shiftid = 230303001
        shiftid = get_shiftid_from_timestamp(timestamp, day_shift_start_time)
        self.assertEqual(shiftid, expected_shiftid)

    def test_get_shiftid_from_timestamp_night_shift(self):
        timestamp = datetime(2023, 3, 3, 22, 30, 0)
        day_shift_start_time = time(5, 30, 0)
        expected_shiftid = 230303002
        shiftid = get_shiftid_from_timestamp(timestamp, day_shift_start_time)
        self.assertEqual(shiftid, expected_shiftid)

    def test_get_shiftid_from_timestamp_past_midnight(self):
        timestamp = datetime(2023, 3, 4, 2, 30, 0)
        day_shift_start_time = time(5, 30, 0)
        expected_shiftid = 230303002
        shiftid = get_shiftid_from_timestamp(timestamp, day_shift_start_time)
        self.assertEqual(shiftid, expected_shiftid)

    def test_get_shiftid_from_timestamp_at_midnight(self):
        timestamp = datetime(2023, 3, 3, 0, 0, 0)
        day_shift_start_time = time(8, 0, 0)
        expected_shiftid = 230302002
        shiftid = get_shiftid_from_timestamp(timestamp, day_shift_start_time)
        self.assertEqual(shiftid, expected_shiftid)

    def test_get_shiftid_from_timestamp_at_end_of_day_shift(self):
        timestamp = datetime(2023, 3, 3, 19, 59, 59, 999999)
        day_shift_start_time = time(8, 0, 0)
        expected_shiftid = 230303001
        shiftid = get_shiftid_from_timestamp(timestamp, day_shift_start_time)
        self.assertEqual(shiftid, expected_shiftid)

    def test_timestamp_at_start_of_night_shift(self):
        timestamp = datetime(2023, 3, 3, 17, 30, 0)
        day_shift_start_time = time(17, 30)
        expected_shiftid = 230303002
        shiftid = get_shiftid_from_timestamp(timestamp, day_shift_start_time)
        self.assertEqual(shiftid, expected_shiftid)

class GetDateRangeTests(unittest.TestCase):
    
    def test_get_date_range_multiple_dates(self):
        start_date = date(2023, 1, 1)
        end_date = date(2023, 1, 3)
        actual = get_date_range(start_date, end_date)
        expected = [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)]
        self.assertEqual(actual, expected)

    def test_get_date_range_one_date(self):
        start_date = date(2023, 1, 1)
        end_date = date(2023, 1, 1)
        actual = get_date_range(start_date, end_date)
        expected = [date(2023, 1, 1)]
        self.assertEqual(actual, expected)

    def test_get_date_range_wrong_order(self):
        start_date = date(2023, 1, 3)
        end_date = date(2023, 1, 1)
        actual = get_date_range(start_date, end_date)
        expected = []
        self.assertEqual(actual, expected)
