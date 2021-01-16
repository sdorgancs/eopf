from __future__ import annotations

import os.path
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import IntEnum
from os import path
from tempfile import mkdtemp
from typing import IO, Any, Dict, List, Optional, Union

from fs import open_fs


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
            if etype == EntryType.FILE:
                result[entry] = None
            if etype == EntryType.DIRECTORY:
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
    def __init__(self, service_url: str, credentials: Optional[Credentials]) -> None:
        super().__init__(service_url, credentials=credentials)
        self.FS = open_fs(service_url, writeable=True)

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

    @abstractmethod
    def download(self, remote_path: str, local_path: str) -> None:
        """download downloads the content of a remote path into the local file system

        :param remote_path: the path of the remote file or directory
        :type remote_path: str
        :param local_path: a path in the local file system
        :type local_path: str
        """
        etype = self.entry_type(remote_path)
        if etype == EntryType.FILE:
            if path.isfile(local_path):
                with open(local_path, "wb") as local_file:
                    self.FS.download(remote_path, local_file)
            elif path.isdir(local_path):
                fname = path.basename(remote_path)
                with open(path.join(local_path, fname), "wb") as local_file:
                    self.FS.download(remote_path, local_file)
            else:
                raise ValueError(
                    "if remote_path is a file, local_path can only be a file or a dir"
                )
        if etype == EntryType.DIRECTORY and path.isdir(local_path):
            listdir = self.ls(remote_path)
            files = [
                (fname, self.entry_type(path.join(remote_path, fname)))
                for fname in listdir
            ]
            for (fname, etype) in files:
                if etype == EntryType.FILE or etype == EntryType.DIRECTORY:
                    self.download(
                        path.join(remote_path, fname), path.join(local_path, fname)
                    )
            # dirs =  [dname for dname in self.ls(remote_path) if self.is_file(dname)]
        else:
            raise ValueError("if remote_path is a dir, local path can only be a dir")

    @abstractmethod
    def upload(self, local_path: str, remote_path: str) -> None:
        """[summary]
        :param local_path: a path in the local file system
        :type local_path: str
        :param remote_path: the path in this Storage where the content of local_path will be copied
        :type remote_path: str

        """

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

    @abstractmethod
    def mkdir(self, remote_path: str):
        """mkdir creates a directory named remote_path in this Storage

        :param remote_path: the complete path of the directory to create
        :type remote_path: str
        """

    @abstractmethod
    def remove(self, remote_path: str):
        """remove removes the remote_path from this Storage (file or directory)

        :param remote_path: [description]
        :type remote_path: str
        """

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
