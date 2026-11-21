# unit tests:  python -m unittest discover -s tests -v

import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

import transform 
import validate  
from common import get_spark  

SCHEMA = ("country_code string, country_name string, month date, tourist_nights long, "
          "avg_temp_c double, total_precip_mm double, sunshine_hours double, "
          "rainy_days long, weather_days long")
GOOD_ROW = ("HU", "Hungary", date(2024, 7, 1), 3_000_000, 26.1, 40.2, 300.5, 5, 31)


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark("tests")

    def curated(self, rows):
        return self.spark.createDataFrame(rows, SCHEMA)

    def test_split_valid_keeps_bad_rows_with_reason(self):
        df = self.spark.createDataFrame([(1,), (-5,), (None,)], "nights int")
        valid, rejected = transform.split_valid(df, [
            ("missing", F.col("nights").isNull()),
            ("negative", F.col("nights") < 0),
        ])
        self.assertEqual(valid.count(), 1)
        reasons = sorted(r.reject_reason for r in rejected.collect())
        self.assertEqual(reasons, ["missing", "negative"])

    def test_duplicate_key_fails(self):
        with self.assertRaises(validate.DataQualityError):
            validate.validate_curated(self.curated([GOOD_ROW, GOOD_ROW]))

    def test_missing_weather_fails_join_check(self):
        no_weather = GOOD_ROW[:4] + (None, None, None, 0, 0)
        with self.assertRaisesRegex(validate.DataQualityError, "join check"):
            validate.validate_curated(self.curated([no_weather]))

    def test_good_data_passes(self):
        validate.validate_curated(self.curated([GOOD_ROW]))


if __name__ == "__main__":
    unittest.main()