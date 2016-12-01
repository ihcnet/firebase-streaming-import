from oauth2client.service_account import ServiceAccountCredentials


def get_access_token(json_auth_file):
    scopes = [
        'https://www.googleapis.com/auth/firebase.database',
        'https://www.googleapis.com/auth/userinfo.email'
    ]
    cred = ServiceAccountCredentials.from_json_keyfile_name(json_auth_file, scopes)
    return cred.get_access_token()
