import uuid

class BaseMsg:
    def __init__(self, origin='', destination=''):
        self.uid = uuid.uuid4()
        self.origin = origin
        self.destination = destination

    def type(self):
        return self.__class__.__name__


class ReplicationMsg(BaseMsg):
    def __init__(self, data, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data = data


class SetMsg(BaseMsg):
    def __init__(self, key, value, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key
        self.value = value


class SetMsgResponse(BaseMsg):
    def __init__(self, orig_uid, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orig_uid = orig_uid


class GetMsg(BaseMsg):
    def __init__(self, key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key


class GetMsgResponse(BaseMsg):
    def __init__(self, orig_uid, value, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orig_uid = orig_uid
        self.value = value


class GetOwners(BaseMsg):
    def __init__(self, key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key


class GetOwnersResponse(BaseMsg):
    def __init__(self, key, owner_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key
        self.owner_id = owner_id
