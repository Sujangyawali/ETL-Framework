from lib.logger import Logger
import pysftp

class SFTP:
    def __init__(self, host, post, username, password = None, keyfile = None, log = Logger):
        self.host = host
        self.port = post
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
                self.sftp = pysftp.Connection(host = self.host, username = self.username, password = self.password, cnopts= cnopts)
            self.log.log_message("Connection to SFTP server established")
        except Exception as ce:
            self.log.log_message(f"Error connecting to SFTP server: " + str(ce))
            raise Exception(f"Error connecting to SFTP server:" + str(ce))
        

    def get_file(self, dir): #extracts one file per directory
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

    def put_file(self):
        pass

    def close(self):
        pass
        
