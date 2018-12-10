import ConfigParser


class Config:

    def __init__(self):
        config = ConfigParser.RawConfigParser()

        config.read("env.properties")
        self.__db_host = config.get("prod", "db.host")
        self.__db_name = config.get("prod", "db.name")
        self.__db_user = config.get("prod", "db.user")
        self.__db_password = config.get("prod", "db.password")

        self.__ssh_key_file = config.get("prod", "ssh.key.file")

    @property
    def db_host(self):
        return self.__db_host

    @property
    def db_name(self):
        return self.__db_name

    @property
    def db_user(self):
        return self.__db_user

    @property
    def db_password(self):
        return self.__db_password

    @property
    def ssh_key_file(self):
        return self.__ssh_key_file
