"""AAA regressions for truthful fuzz discovery and execution failures."""
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import run_fuzz


class FuzzRunnerTests(unittest.TestCase):
    def test_all_modules_and_each_exact_target(self):
        # Arrange: two modules with two probes in one package and no probes in another.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "go.mod").write_text("module root\n")
            adapter = root / "adapter"
            adapter.mkdir()
            (adapter / "go.mod").write_text("module adapter\n")
            answers = ["adapter\n", "ok adapter\n", "root\n", "FuzzOne\nFuzzTwo\nok root\n"]
            with mock.patch.object(subprocess, "check_output", side_effect=answers), \
                    mock.patch.object(subprocess, "run") as execute:
                # Act.
                run_fuzz.run(root, 3, 2)
                # Assert: no ambiguous -fuzz=. match and checked failure propagation.
                self.assertEqual(execute.call_count, 2)
                names = [call.args[0][3] for call in execute.call_args_list]
                self.assertEqual(names, ["-fuzz=^FuzzOne$", "-fuzz=^FuzzTwo$"])
                self.assertTrue(all(call.kwargs["check"] for call in execute.call_args_list))

    def test_empty_probe_discovery_fails(self):
        # Arrange.
        with mock.patch.object(run_fuzz, "modules", return_value=[Path("/tmp/module")]), \
                mock.patch.object(subprocess, "check_output", side_effect=["package\n", "ok package\n"]):
            # Act / Assert: modules without probes cannot report coverage success.
            with self.assertRaisesRegex(ValueError, "no fuzz probes"):
                run_fuzz.discover(Path("/tmp"))

    def test_discovery_error_is_not_no_probes_success(self):
        # Arrange.
        failure = subprocess.CalledProcessError(1, ["go", "list"])
        with mock.patch.object(run_fuzz, "modules", return_value=[Path("/tmp/module")]), \
                mock.patch.object(subprocess, "check_output", side_effect=failure):
            # Act / Assert.
            with self.assertRaises(subprocess.CalledProcessError):
                run_fuzz.discover(Path("/tmp"))

    def test_probe_failure_stops_run(self):
        # Arrange.
        probe = (Path("/tmp/module"), "package", "FuzzFail")
        failure = subprocess.CalledProcessError(1, ["go", "test"])
        with mock.patch.object(run_fuzz, "discover", return_value=[probe]), \
                mock.patch.object(subprocess, "run", side_effect=failure):
            # Act / Assert.
            with self.assertRaises(subprocess.CalledProcessError):
                run_fuzz.run(Path("/tmp"), 3, 2)


if __name__ == "__main__":
    unittest.main()
