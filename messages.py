class BaseMsg:
    def __init__(self, origin='', destination=''):
        self.origin = origin
        self.destination = destination

class TextMsg(BaseMsg):
    def __init__(self, msg, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.msg = msg
