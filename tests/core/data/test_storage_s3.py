import pytest
import os.path as path
import shutil
import json


@pytest.fixture
def s3_fs():
    from eopf.core.data.storage import MuliStorage

    return MuliStorage(
        "s3://testbucket?endpoint_url=http://localhost:5000&region=us-east-1"
    )


def test_connection(s3_fs):
    files = s3_fs.ls("/")
    assert len(files) == 1
    assert files[0] == "README.md"


def test_mkdir_upload(s3_fs):
    if not s3_fs.exists("/config"):
        s3_fs.mkdir("/config")
    s3_fs.upload(".vscode", "/config/vscode")
    files = s3_fs.ls("/config/vscode")
    assert len(files) > 0
    assert s3_fs.exists("/config/vscode/launch.json")
    assert s3_fs.exists("/config/vscode/settings.json")


def test_download(s3_fs):
    test_mkdir_upload(s3_fs)
    s3_fs.download("/config/vscode", "test")
    assert path.exists("test/launch.json")
    assert path.exists("test/settings.json")
    shutil.rmtree("test")
    s3_fs.remove("/config/vscode/launch.json")
    assert not s3_fs.exists("/config/vscode/launch.json")
    s3_fs.remove("/config")
    assert not s3_fs.exists("/config")


def test_open(s3_fs):
    s3_fs.upload("cli_examples/concat.json", "/concat.json")
    with s3_fs.open("/concat.json") as f:
        concat = json.load(f)
        assert "strings" in concat
    s3_fs.remove("/concat.json")
