import confluent_kafka.admin as kafka_admin

class acl_binding_options:
    def __init__(self, admin: kafka_admin.AdminClient):
        self.admin = admin
        self.topic: str = ""
        self.group: str = ""
        self.principal: str = ""
        self.consumer: bool = None
        self.producer: bool = None
        self.operations: list[kafka_admin.AclOperation] = []
        self.all: bool = False
        self.prefixed: bool = False

    def set_topic(self, topic: str, prefixed: bool = False):
        self.topic = topic

    def set_group(self, group: str, prefixed: bool = False):
        self.group = group

    def set_principal(self, principal: str):
        self.principal = principal

    def set_consumer(self, consumer: bool = True):
        self.consumer = consumer

    def set_producer(self, producer: bool = True):
        self.producer = producer

    def set_operations(self, operations: list):
        self.operations = operations

    def set_all(self, all: bool = True):
        self.all = all
    
    def set_prefixed(self, prefixed: bool = True):
        self.prefixed = prefixed