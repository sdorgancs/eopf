from __future__ import annotations

import os
import stat
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import IntEnum
from os import path
from tempfile import mkdtemp
from typing import IO, Any, Dict, List, Optional, Union

from fs import open_fs
from fs.enums import ResourceType
from fs_s3fs import S3FS


@dataclass
class BasicAuthentication:
    """BasicAuthentication is used to connect services using a username/password pair (LDAP, SQL Database, ...)
    """

    username: str
    password: str


@dataclass
class AuthenticationByToken:
    """AuthenticationByToken is used to connect services using authentication token (OpenID, OAuth2, Kerberos...)
    """

    token: str


Credentials = Union[BasicAuthentication, AuthenticationByToken]
"""Credentials stores user credentials to access a service. To authentication type are allowed:
- BasicAuthentication: to connect services using a pair username/password (LDAP, SQL Database, ...)
- AuthenticationByToken: to connect services using authentication token (OpenID, OAuth2, Kerberos...)
"""


class EntryType(IntEnum):
    UNKNOWN = 0
    DIRECTORY = 1
    FILE = 2
    CHARACTER = 3
    BLOCK_SPECIAL_FILE = 4
    FIFO = 5
    SOCKET = 6
    SYMLINK = 7


class StorageAPI(ABC):
    """StorageAPI is an abstract class defining the methods for interacting with the EOPF external storages
    """

    def __init__(
        self,
        service_url: str,
        credentials: Optional[Credentials] = None,
        writable: bool = False,
    ) -> None:
        self.service_url = service_url
        self.credentials = credentials
        self.writable = writable

    def copyto(
        self, source_path: str, storage: StorageAPI, destination_path: str
    ) -> None:
        """copyto copies a file or a directory from a storage to another
        This method is not abstract, this naïve implementation is provided as an exemple to be optimized
        :param source_path: source path in self Storage
        :type source_path: str
        :param storage: another storage where the content of source_path
        :type storage: StorageAPI
        :param destination_path: the path where the content of source_path will be copied in the other storage
        :type destination_path: str
        """
        if not storage.writable:
            raise ValueError(f"Storage {self.service_url} is not writable")
        tmp = mkdtemp()
        tail = os.path.split(source_path)[1]
        self.download(source_path, tmp)
        storage.upload(os.path.join(tmp, tail), destination_path)
        shutil.rmtree(tmp)

    @abstractmethod
    def download(self, remote_path: str, local_path: str) -> None:
        """download downloads the content of a remote path into the local file system

        :param remote_path: the path of the remote file or directory
        :type remote_path: str
        :param local_path: a path in the local file system
        :type local_path: str
        """

    def upload(self, local_path: str, remote_path: str) -> None:
        """[summary]
        :param local_path: a path in the local file system
        :type local_path: str
        :param remote_path: the path in this Storage where the content of local_path will be copied
        :type remote_path: str

        """
        if not self.writable:
            raise ValueError(f"Storage {self.service_url} is not writable")

    @abstractmethod
    def ls(self, remote_dir: str) -> List[str]:
        """ls a list containing the names of the entries in the directory given by remote path

        :param remote_dir: the path of the remote directory
        :type remote_dir: str
        :return: the list of the files and directory of remote path
        :rtype: List[str]
        """

    @abstractmethod
    def exists(self, remote_path: str) -> bool:
        """exists checks if remote_dir exists in this Storage

        :param remote_path: the path of the remote entry
        :type remote_path: str
        :return: True if remote_path exists
        :rtype: bool
        """

    @abstractmethod
    def entry_type(self, remote_path: str) -> EntryType:
        """Returns the type of entry referenced by remote_path

        :param remote_path: the path of the remote entry
        :type remote_path: str
        :return: one of the possible entry type
        :rtype: EntryType
        """

    def tree(self, remote_dir: str) -> Dict[str, Any]:
        """tree returns the remote_dir file and directory tree.
        This method is not abstract, this naïve implementation is provided as an exemple to be optimized
        :param remote_dir: the path of the remote resource
        :type remote_dir: str
        :return: the remote_dir file and directory tree store in a dict like the following
         {
            "file1":None,
            "dir1": {
                "file2": None
            }
        }
        :rtype: Dict[str, Any]
        """
        entries = self.ls(remote_dir)
        result: Dict[str, Any] = {}
        for entry in entries:
            etype = self.entry_type(entry)
            if etype is EntryType.FILE:
                result[entry] = None
            if etype is EntryType.DIRECTORY:
                result[entry] = self.tree(os.path.join(remote_dir, entry))
        return result

    def mkdir(self, remote_path: str):
        """mkdir creates a directory named remote_path in this Storage

        :param remote_path: the complete path of the directory to create
        :type remote_path: str
        """
        if not self.writable:
            raise ValueError(f"Storage {self.service_url} is not writable")

    def remove(self, remote_path: str):
        """remove removes the remote_path from this Storage (file or directory)

        :param remote_path: [description]
        :type remote_path: str
        """
        if not self.writable:
            raise ValueError(f"Storage {self.service_url} is not writable")

    def open(
        self, remote_file: str, mode: str = "r", encoding: str = "None"
    ) -> IO[Any]:
        """Open an encoded file using the given mode and return an instance of StreamReaderWriter, providing transparent encoding/decoding
         This method is not abstract, this naïve implementation is provided as an exemple to be optimized

        :param remote_file: the file to open in this Storage
        :type remote_file: str
        :param mode: opening mode, see Python built-in open function, defaults to 'r'
        :type mode: str, optional
        :param encoding: specifies the encoding which is to be used for the file. Any encoding that encodes to and decodes from bytes is allowed, defaults to "None"
        :type encoding: str, optional
        :return: a Python file object like
        :rtype: StreamReaderWriter
        """
        tmp = mkdtemp()
        basename = os.path.basename(remote_file)
        self.download(remote_file, tmp)
        srw = open(os.path.join(tmp, basename), mode=mode, encoding=encoding)

        def close_delete(self) -> None:
            shutil.rmtree(tmp)
            self.close()

        setattr(srw.__class__, "close", close_delete)
        return srw


class MuliStorage(StorageAPI):
    def __init__(
        self, service_url: str, credentials: Optional[Credentials] = None
    ) -> None:
        super().__init__(service_url, credentials=credentials)
        self.remote_fs = open_fs(service_url, writeable=True)

        # TODO remove this patch when when https://github.com/PyFilesystem/s3fs/issues/83 is closed
        if isinstance(self.remote_fs, S3FS):
            from urllib.parse import urlparse, parse_qs

            url = urlparse(service_url)
            params = parse_qs(url.query)
            self.remote_fs.region = params["region"][0]

    def copyto(
        self, source_path: str, storage: StorageAPI, destination_path: str
    ) -> None:
        """copyto copies a file or a directory from a storage to another
        This method is not abstract, this naïve implementation is provided as an exemple to be optimized
        :param source_path: source path in self Storage
        :type source_path: str
        :param storage: another storage where the content of source_path
        :type storage: StorageAPI
        :param destination_path: the path where the content of source_path will be copied in the other storage
        :type destination_path: str
        """
        tmp = mkdtemp()
        tail = os.path.split(source_path)[1]
        self.download(source_path, tmp)
        storage.upload(os.path.join(tmp, tail), destination_path)
        shutil.rmtree(tmp)

    def _check_download_inputs(self, remote_path: str, local_path: str) -> EntryType:
        remote_path_type = self.entry_type(remote_path)
        if not path.exists(local_path) and remote_path_type is EntryType.DIRECTORY:
            os.makedirs(local_path)
        assert self.exists(remote_path), "remote_path must exist"

        local_path_type = _local_entry_type(local_path)
        assert local_path_type is EntryType.DIRECTORY, "local path must be a directory"
        assert (
            remote_path_type is EntryType.DIRECTORY
            or remote_path_type is EntryType.FILE
        ), "remote path must be a file or a directory"
        return remote_path_type

    def download(self, remote_path: str, local_path: str) -> None:
        """download downloads the content of a remote path into the local file system

        :param remote_path: the path of the remote file or directory
        :type remote_path: str
        :param local_path: a local directory
        :type local_path: str
        """
        # check input parameters
        remote_path_type = self._check_download_inputs(remote_path, local_path)

        if remote_path_type is EntryType.FILE:
            fname = path.basename(remote_path)
            with open(path.join(local_path, fname), "wb") as local_file:
                self.remote_fs.download(remote_path, local_file)
        if remote_path_type is EntryType.DIRECTORY:
            for fname in self.ls(remote_path):
                new_remote_path = path.join(remote_path, fname)
                new_remote_path_type = self.entry_type(new_remote_path)
                if new_remote_path_type is EntryType.FILE:
                    self.download(new_remote_path, local_path)
                elif new_remote_path_type is EntryType.DIRECTORY:
                    new_local_dir = path.join(local_path, fname)
                    os.mkdir(new_local_dir)
                    self.download(new_remote_path, new_local_dir)

    def _check_upload_inputs(self, local_path: str, remote_path: str) -> EntryType:
        assert path.exists(local_path), "local path must exist"
        local_path_type = _local_entry_type(local_path)
        if not self.exists(remote_path) and local_path_type is EntryType.DIRECTORY:
            self.mkdir(remote_path)
            remote_path_type = self.entry_type(remote_path)

            assert (
                remote_path_type is EntryType.DIRECTORY
            ), "remote path must be a directory"
            assert (
                local_path_type is EntryType.DIRECTORY
                or local_path_type is EntryType.FILE
            ), "local path must be a file or a directory"
        return local_path_type

    def upload(self, local_path: str, remote_path: str) -> None:
        """Uploads a file or a directory into the storage
        :param local_path: a local file or directory
        :type local_path: str
        :param remote_path: a directory path is the storage
        :type remote_path: str

        """
        local_path_type = self._check_upload_inputs(local_path, remote_path)

        if local_path_type is EntryType.FILE:
            fname = path.basename(local_path)
            with open(local_path, "rb") as file:
                # remote path is intended to to a directory and it does not exists
                if remote_path[-1] == "/" and not self.exists(remote_path):
                    self.mkdir(remote_path)
                self.remote_fs.upload(remote_path, file)

        elif local_path_type is EntryType.DIRECTORY:
            if not self.exists(remote_path):
                self.mkdir(remote_path)
            for fname in os.listdir(local_path):
                new_local_path = path.join(local_path, fname)
                new_local_path_type = _local_entry_type(new_local_path)

                if new_local_path_type is EntryType.FILE:
                    new_remote_file = path.join(remote_path, fname)
                    self.upload(new_local_path, new_remote_file)

                elif new_local_path_type is EntryType.DIRECTORY:
                    new_remote_dir = path.join(remote_path, fname)
                    self.mkdir(new_remote_dir)
                    self.upload(new_local_path, new_remote_dir)

    def ls(self, remote_dir: str) -> List[str]:
        """ls a list containing the names of the entries in the directory given by remote path

        :param remote_dir: the path of the remote directory
        :type remote_dir: str
        :return: the list of the files and directory of remote path
        :rtype: List[str]
        """
        return self.remote_fs.listdir(remote_dir)

    def exists(self, remote_path: str) -> bool:
        """exists checks if remote_dir exists in this Storage

        :param remote_path: the path of the remote entry
        :type remote_path: str
        :return: True if remote_path exists
        :rtype: bool
        """
        return self.remote_fs.exists(remote_path)

    def entry_type(self, remote_path: str) -> EntryType:
        """Returns the type of entry referenced by remote_path

        :param remote_path: the path of the remote entry
        :type remote_path: str
        :return: one of the possible entry type
        :rtype: EntryType
        """
        remote_type = self.remote_fs.getinfo(remote_path, namespaces=["details"]).type
        if remote_type == ResourceType.directory:
            return EntryType.DIRECTORY
        if remote_type == ResourceType.character:
            return EntryType.CHARACTER
        if remote_type == ResourceType.block_special_file:
            return EntryType.BLOCK_SPECIAL_FILE
        if remote_type == ResourceType.file:
            return EntryType.FILE
        if remote_type == ResourceType.fifo:
            return EntryType.FIFO
        if remote_type == ResourceType.symlink:
            return EntryType.SYMLINK
        if remote_type == ResourceType.socket:
            return EntryType.SOCKET
        else:
            return EntryType.UNKNOWN

    def mkdir(self, remote_path: str):
        """mkdir creates a directory and missing directories named remote_path in this Storage

        :param remote_path: the complete path of the directory to create
        :type remote_path: str
        """
        fs = self.remote_fs.makedirs(remote_path, recreate=False)
        assert fs is not None

    def remove(self, remote_path: str):
        """remove removes the remote_path from this Storage (file or directory)

        :param remote_path: [description]
        :type remote_path: str
        """
        if self.entry_type(remote_path) is EntryType.FILE:
            self.remote_fs.remove(remote_path)
        else:
            self.remote_fs.removetree(remote_path)

    def open(
        self, remote_file: str, mode: str = "r", encoding: str = "None"
    ) -> IO[Any]:
        """Open an encoded file using the given mode and return an instance of StreamReaderWriter, providing transparent encoding/decoding
         This method is not abstract, this naïve implementation is provided as an exemple to be optimized

        :param remote_file: the file to open in this Storage
        :type remote_file: str
        :param mode: opening mode, see Python built-in open function, defaults to 'r'
        :type mode: str, optional
        :param encoding: specifies the encoding which is to be used for the file. Any encoding that encodes to and decodes from bytes is allowed, defaults to "None"
        :type encoding: str, optional
        :return: a Python file object like
        :rtype: StreamReaderWriter
        """
        return self.remote_fs.open(remote_file, mode=mode)


def _local_entry_type(local_path: str) -> EntryType:
    mode = os.stat(local_path, follow_symlinks=False).st_mode
    if stat.S_ISDIR(mode) != 0:
        return EntryType.DIRECTORY
    if stat.S_ISCHR(mode) != 0:
        return EntryType.CHARACTER
    if stat.S_ISBLK(mode) != 0:
        return EntryType.BLOCK_SPECIAL_FILE
    if stat.S_ISREG(mode) != 0:
        return EntryType.FILE
    if stat.S_ISFIFO(mode) != 0:
        return EntryType.FIFO
    if stat.S_ISLNK(mode) != 0:
        return EntryType.SYMLINK
    if stat.S_ISSOCK(mode) != 0:
        return EntryType.SOCKET
    else:
        return EntryType.UNKNOWN
