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
            self.log.log_message("Error connecting to SFTP server: " + str(ce))
            raise Exception(f"Error connecting to SFTP server:" + str(ce))
    
    def get_file(self):
        pass

    def put_file(self):
        pass

    def close(self):
        pass
        
