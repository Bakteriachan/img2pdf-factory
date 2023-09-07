import requests

class SessionService:
    def __init__(self, sessions_url: str):
        self.sessions_url = sessions_url
        if self.sessions_url.endswith('/'):
            self.sessions_url = self.sessions_url[:-1]

    def get_session_by_id(self, session_id: str):
        if not session_id:
            raise Exception('session_id is None')
        
        url = f'{self.sessions_url}/{session_id}'

        res = requests.get(url)
        if res.status_code > 299 and res.status_code < 200:
            raise Exception(res.text)

        return res.json()
