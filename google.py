from oauth2client.service_account import ServiceAccountCredentials
import json


class JWTProvider:
    def __init__(self, filename):
        self.filename = filename
        self.scopes = [
            'https://www.googleapis.com/auth/firebase.database',
            'https://www.googleapis.com/auth/userinfo.email'
        ]
        with open(filename) as json_file:
            self.key_file_dict = json.load(json_file)

    def get_credentials(self):
        return ServiceAccountCredentials.from_json_keyfile_dict(self.key_file_dict, self.scopes)
