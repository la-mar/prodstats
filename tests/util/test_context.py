import os

import util.context as ctx


class TestWorkingDirectory:
    def test_reverts_on_exit(self, tmpdir):
        expected = os.getcwd()
        with ctx.working_directory(tmpdir):
            assert os.getcwd() == tmpdir
        assert os.getcwd() == expected
