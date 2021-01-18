import pytest
import os.path as path
import os
import shutil
import json


@pytest.fixture(scope='session')
def local_fs():
    from eopf.core.data.storage import MuliStorage
    if path.exists('local-test'):
        shutil.rmtree('local-test')
    os.mkdir('local-test')
    shutil.copy('README.md', 'local-test')
    yield MuliStorage("osfs://local-test")
    shutil.rmtree('local-test')

def test_connection(local_fs):
    files = local_fs.ls("/")
    assert len(files) == 1
    assert files[0] == "README.md"


def test_mkdir_upload(local_fs):
    if not local_fs.exists("/config"):
        local_fs.mkdir("/config")
    local_fs.upload(".vscode", "/config/vscode")
    files = local_fs.ls("/config/vscode")
    assert len(files) > 0
    assert local_fs.exists("/config/vscode/launch.json")
    assert local_fs.exists("/config/vscode/settings.json")


def test_download(local_fs):
    test_mkdir_upload(local_fs)
    local_fs.download("/config/vscode", "test")
    assert path.exists("test/launch.json")
    assert path.exists("test/settings.json")
    shutil.rmtree("test")
    local_fs.remove("/config/vscode/launch.json")
    assert not local_fs.exists("/config/vscode/launch.json")
    local_fs.remove("/config")
    assert not local_fs.exists("/config")


def test_open(local_fs):
    local_fs.upload("cli_examples/concat.json", "/concat.json")
    with local_fs.open("/concat.json") as f:
        concat = json.load(f)
        assert "strings" in concat
    local_fs.remove("/concat.json")
