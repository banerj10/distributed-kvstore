from hashlib import md5

class Store:
    hash_table = {}
    replicas = {}

    @staticmethod
    def hash(key):
        return int(md5(key.encode()).hexdigest(), base=16) % 10
