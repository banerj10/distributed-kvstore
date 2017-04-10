import uuid

class BaseMsg:
    def __init__(self, origin='', destination=''):
        self.uid = uuid.uuid4()
        self.origin = origin
        self.destination = destination

class TextMsg(BaseMsg):
    def __init__(self, msg, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.msg = msg

class SetMsg(BaseMsg):
    def __init__(self, key, value, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key
        self.value = value

class SetMsgResponse(BaseMsg):
    def __init__(self, orig_uid, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orig_uid = orig_uid
