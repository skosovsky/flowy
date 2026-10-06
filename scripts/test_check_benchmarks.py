import unittest

from check_benchmarks import check


class BenchmarkGateTests(unittest.TestCase):
    def setUp(self):
        # Arrange: a fixed allocation contract, independent of measured output.
        self.manifest = {"benchmarks": {"BenchmarkNode": {"B/op": 16, "allocs/op": 1}}}
        self.valid = "BenchmarkNode-8 100 45.2 ns/op 16 B/op 1 allocs/op\nPASS\n"

    def test_measured_case_passes(self):
        # Act, Assert.
        self.assertEqual(check(self.manifest, self.valid), [])

    def test_empty_or_missing_benchmark_fails(self):
        for output in ("", "PASS\n", self.valid.replace("BenchmarkNode", "BenchmarkOther")):
            with self.subTest(output=output):
                self.assertTrue(check(self.manifest, output))

    def test_each_allocation_limit_is_enforced(self):
        for output in (self.valid.replace("16 B/op", "17 B/op"), self.valid.replace("1 allocs/op", "2 allocs/op")):
            with self.subTest(output=output):
                self.assertTrue(check(self.manifest, output))

    def test_skip_failure_zero_iterations_and_invalid_numbers_fail(self):
        for output in (
            "BenchmarkNode\nPASS\n", self.valid + "FAIL\n", self.valid.replace("100", "0"),
            self.valid.replace("16 B/op", "nan B/op"), self.valid.replace("16 B/op", "-1 B/op"),
            self.valid.replace("B/op", "bytes"), self.valid.replace("PASS", ""),
        ):
            with self.subTest(output=output):
                self.assertTrue(check(self.manifest, output))

    def test_failed_duplicate_cannot_be_hidden_by_valid_repeat(self):
        self.assertTrue(check(self.manifest, self.valid.replace("16 B/op", "17 B/op") + self.valid))


if __name__ == "__main__":
    unittest.main()
