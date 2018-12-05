
class FileInfo:

    def __init__(self, isolate_record, genus, species):
        self.__isolate_record = isolate_record
        self.__genus = genus
        self.__species = species

    @property
    def isolate_record(self):
        return self.__isolate_record

    @property
    def genus(self):
        return self.__genus

    @property
    def species(self):
        return self.__species

