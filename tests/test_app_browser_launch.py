from __future__ import annotations

import os
import sys
import unittest
from unittest import mock

from meshtracer_app.app import _browser_launch_env


class BrowserLaunchEnvTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_env = os.environ.copy()

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._old_env)

    def test_linux_frozen_uses_ld_library_path_orig(self) -> None:
        os.environ["LD_LIBRARY_PATH"] = "/tmp/_MEI123"
        os.environ["LD_LIBRARY_PATH_ORIG"] = "/usr/lib/x86_64-linux-gnu"

        with mock.patch.object(sys, "platform", "linux"), mock.patch.object(
            sys, "frozen", True, create=True
        ):
            with _browser_launch_env():
                self.assertEqual(
                    os.environ.get("LD_LIBRARY_PATH"),
                    "/usr/lib/x86_64-linux-gnu",
                )
            self.assertEqual(os.environ.get("LD_LIBRARY_PATH"), "/tmp/_MEI123")

    def test_linux_frozen_removes_ld_library_path_when_orig_missing(self) -> None:
        os.environ["LD_LIBRARY_PATH"] = "/tmp/_MEI123"
        os.environ.pop("LD_LIBRARY_PATH_ORIG", None)

        with mock.patch.object(sys, "platform", "linux"), mock.patch.object(
            sys, "frozen", True, create=True
        ):
            with _browser_launch_env():
                self.assertNotIn("LD_LIBRARY_PATH", os.environ)
            self.assertEqual(os.environ.get("LD_LIBRARY_PATH"), "/tmp/_MEI123")

    def test_non_linux_runtime_keeps_ld_library_path(self) -> None:
        os.environ["LD_LIBRARY_PATH"] = "/tmp/_MEI123"
        os.environ["LD_LIBRARY_PATH_ORIG"] = "/usr/lib/x86_64-linux-gnu"

        with mock.patch.object(sys, "platform", "win32"), mock.patch.object(
            sys, "frozen", True, create=True
        ):
            with _browser_launch_env():
                self.assertEqual(os.environ.get("LD_LIBRARY_PATH"), "/tmp/_MEI123")
            self.assertEqual(os.environ.get("LD_LIBRARY_PATH"), "/tmp/_MEI123")


if __name__ == "__main__":
    unittest.main()
