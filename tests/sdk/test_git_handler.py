import datetime
import functools
from pathlib import Path
from unittest.mock import patch, MagicMock

from databricks_terraformer.sdk.git_handler import GitHandler

import datetime


class MockNow(datetime.datetime):
    @classmethod
    def now(cls):
        return cls(2010, 1, 1)


class TestGitHandler:

    @patch('pathlib.Path.absolute', return_value=True)
    @patch('git.Repo.clone_from', return_value=MagicMock())
    def test_stage_changes(self, git_mock, path_mock):
        gh = GitHandler(git_url="fake-git-url", base_path=Path("/tmp/test"))
        gh.stage_changes()

        assert path_mock.called is True
        assert git_mock.return_value.git.add.called is True
        assert git_mock.return_value.git.add.call_args[1]["A"] is True

    @patch('pathlib.Path.absolute', return_value=True)
    @patch('git.Repo.clone_from', return_value=MagicMock())
    def test_commit(self, git_mock, path_mock):
        gh = GitHandler(git_url="fake-git-url", base_path=Path("/tmp/test"))
        datetime.datetime = MockNow
        git_mock.return_value.remote.return_value = MagicMock()
        gh.commit()

        assert path_mock.called is True

        # Commit and push to remote
        assert git_mock.return_value.index.commit.called is True
        assert git_mock.return_value.index.commit.call_args[0][0] == \
               "Updated via databricks-sync."
        assert git_mock.return_value.remote.called is True
        assert git_mock.return_value.remote.return_value.push.called is True
        assert git_mock.return_value.remote.return_value.push.call_args[0][0] == "--no-verify"

        # Push tags
        expected_tag = "v20100101000000000000"
        assert git_mock.return_value.create_tag.called is True
        assert git_mock.return_value.create_tag.call_args[0][0] == expected_tag
        assert git_mock.return_value.create_tag.call_args[1]['message'] == f'Updated with tag "{expected_tag}"'

        assert git_mock.return_value.git.push.called is True
        assert git_mock.return_value.git.push.call_args[0][0] == "origin"
        assert git_mock.return_value.git.push.call_args[0][1] == expected_tag
        assert git_mock.return_value.git.push.call_args[0][2] == "--porcelain"
        assert git_mock.return_value.git.push.call_args[0][3] == "--no-verify"
