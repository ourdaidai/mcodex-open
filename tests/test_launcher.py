from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class RepositoryLauncherTests(unittest.TestCase):
    def test_symlinked_launcher_uses_project_virtualenv_outside_repository(self) -> None:
        repository_root = Path(__file__).resolve().parents[1]
        launcher = repository_root / "mcodex"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            symlink = temp_path / "mcodex"
            symlink.symlink_to(launcher)
            result = subprocess.run(
                ["/usr/bin/python3", str(symlink), "up", "--help"],
                cwd=temp_path,
                capture_output=True,
                text=True,
                check=False,
                env={**os.environ, "PATH": "/usr/bin:/bin"},
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("usage: mcodex up", result.stdout)
        self.assertNotIn("ModuleNotFoundError", result.stderr)


if __name__ == "__main__":
    unittest.main()
