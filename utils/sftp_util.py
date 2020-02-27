import fnmatch
from external_lib import paramiko
from ftplib import FTP
import logging


class FTPConnError(Exception):
    pass


class SFTPUtil:
    def __init__(self, hostname, port, username, password='', key_name=''):

        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.key_name = key_name
        # self.source_name = source_name
        self.connection = None
        self.ftp_type = None

    def connect(self, ftp_type):
        self.ftp_type = ftp_type
        # try:
        if ftp_type == 'sftp':
            if self.key_name:
                # TODO
                pass
            else:
                transport = paramiko.Transport((self.hostname, self.port))
                transport.connect(username=self.username, password=self.password)
            self.connection = paramiko.SFTPClient.from_transport(transport)
            logging.info(self.connection)
        else:
            self.connection = FTP(self.hostname).login(user=self.username, passwd=self.password)
        # except:
        #     raise FTPConnError

        return self.connection

    def get_file_list(self, source_directory, file_pattern, ftp_mode):
        
        if self.ftp_type == 'sftp':
            print(source_directory)
            # self.connection.chdir(source_directory)
            lines = []
            for filename in self.connection.listdir(source_directory):
                if fnmatch.fnmatch(filename, file_pattern):
                    lines.append(filename)
            return lines

        else:
            print("spource_directory", source_directory)
            self.connection.set_pasv(ftp_mode != 'active')
            self.connection.cwd(source_directory)
            return self.connection.nlst(file_pattern)