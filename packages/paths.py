import os
from pathlib import Path

class DWHPaths():
    def __init__(self) -> None:

        # Root directory of the project
        self.BASE_DIR = Path(__file__).resolve().parent.parent
        
        # GCP Credentials folder
        self.CREDENTIALS = os.path.join(self.BASE_DIR, 'credentials') 

        # Path to GCP json keys
        self.GCP_KEYS = os.path.join(self.CREDENTIALS, str(os.listdir(self.CREDENTIALS)[0]))

        # Path to AWS json keys
        self.EXTRACT_KEYS = os.path.join(self.CREDENTIALS, str(os.listdir(self.CREDENTIALS)[1]))

