import uuid

class BaseMsg:
    def __init__(self, origin='', destination=''):
        self.uid = uuid.uuid4()
        self.origin = origin
        self.destination = destination

    def type(self):
        return self.__class__.__name__

    def __str__(self):
        return str(vars(self))


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
    def __init__(self, orig_uid, key, is_owner, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orig_uid = orig_uid
        self.key = key
        self.is_owner = is_owner


class StabilizationMsg(BaseMsg):
    def __init__(self, data, designation, rename=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data = data
        self.designation = designation
        self.rename = rename
