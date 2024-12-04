from lib.logger import Logger
import pysftp
from config.env_setup import *

class SFTP:
    def __init__(self, host, port, username, password = None, keyfile = None, log = Logger):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.keyfile = keyfile
        self.log = log


    def connect(self):
        try:
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            if self.keyfile:
                self.sftp = pysftp.Connection(host = self.host, username = self.username, private_key = self.keyfile, cnopts = cnopts)
            else:
                self.sftp = pysftp.Connection(host = self.host, username = self.username, password = self.password)
            self.log.log_message("Connection to SFTP server established")
        except Exception as ce:
            self.log.log_message(f"Error connecting to SFTP server: " + str(ce))
            raise Exception(f"Error connecting to SFTP server:" + str(ce))
        

    def check_file(self, dir): #extracts one file per directory
        try:
            self.log.log_message(f"Checking file in the directory:{dir}")
            files = self.sftp.listdir(dir)
            # incase of multiple files pick latest one
            if len(files)>1:
                latest_file = sorted(files, key=lambda x: x.rsplit('_', 1)[-1])[-1]
                return latest_file
            if len(files) == 1:
                return files[0]
            else:
                return None
        except Exception as fe:
            self.log.log_message(f"Error getting file from SFTP server: " + str(fe))
            raise Exception(f"Error getting file from SFTP server: " + str(fe))

    def get_file(self, server_path):
        self.log.log_message(f"Loading file into local directory")
        try:
            file_name = os.path.basename(server_path)
            local_file_path = os.path.join(DATA_DIR, file_name)
            self.sftp.get(server_path, local_file_path)
            self.log.log_message(f"File '{file_name}' loaded to local directory")
            # self.delete_file_on_server(server_path)
            return local_file_path
        except Exception as file_load_error:
            self.log.log_message(f"Error while copying file into local directory: " + str(file_load_error))
            raise Exception(f"Error while copying file into local directory: " + str(file_load_error))

    def delete_file_on_server(self, server_path):
        self.log.log_message(f"Deleting file from SFTP server")
        try:
            self.sftp.remove(server_path)
            self.log.log_message(f"File '{os.path.basename(server_path)}' deleted from SFTP server")
        except Exception as file_delete_error:
            self.log.log_message(f"Error while deleting file from SFTP server: " + str(file_delete_error))
            raise Exception(f"Error while deleting file from SFTP server: " + str(file_delete_error))

    def close(self):
        if self.sftp:
            self.sftp.close()
        
