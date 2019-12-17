import fnmatch
import paramiko
from ftplib import FTP


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
        try:
            if ftp_type == 'sftp':
                if self.key_name:
                    # TODO
                    pass
                else:
                    transport = paramiko.Transport((hostname, port))
                    transport.connect(username=username, password=password)
                self.connection = paramiko.SFTPClient.from_transport(transport)
            else:
                self.connection = FTP(hostname).login(user=username, passwd=password)
        except:
            raise FTPConnError

        return self.connection

    def get_file_list(self, source_directory, file_pattern, ftp_mode):
        if self.ftp_type == 'sftp':
            self.connection.chdir(source_directory)
            lines = []
            for filename in conn.listdir(source_directory):
                if fnmatch.fnmatch(filename, file_pattern):
                    lines.append(filename)
            return lines
        else:
            self.connection.set_pasv(ftp_mode != 'active')
            self.connection.cwd(source_directory)
            return conn.nlst(file_pattern)



