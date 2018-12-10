
class ProcessSlot:

    def __init__(self, ec2_process_id, hostname, process):
        self.__ec2_process_id = ec2_process_id
        self.__hostname = hostname
        self.__process = process
        self.__ssh_client = None

    @property
    def ec2_process_id(self):
        return self.__ec2_process_id

    @property
    def hostname(self):
        return self.__hostname

    @property
    def process(self):
        return self.__process

    @property
    def ssh_client(self):
        return self.__ssh_client

    @ssh_client.setter
    def ssh_client(self, ssh_client):
        self.__ssh_client = ssh_client

