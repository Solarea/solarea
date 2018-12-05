import sys, getopt


class ArgParser:

    def __init__(self):
        self.__genus = None
        self.__species = None
        self.__processes = []
        self.__files = []
        self.__count = None
        self.__selection_criteria = None

    def parse(self):
        usage_txt = 'Usage: -g <genus> -s <species> -p <processes> ' \
                    '-f <isolate records> -c <count of records to process>'
        try:
            opts, args = getopt.getopt(sys.argv[1:], "hf:g:s:p:c:",
                                       ["genus=", "file=", "species=", "processes=", "count="])
        except getopt.GetoptError:
            print usage_txt
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                print usage_txt
                sys.exit()
            elif opt in ("-g", "--genus"):
                self.__genus = arg
            elif opt in ("-s", "--species"):
                self.__species = arg
            elif opt in ("-p", "--processes"):
                self.__processes = self.validate_and_get_processes(arg)
            elif opt in ("-f", "--files"):
                self.__files = arg.split(",")
            elif opt in ("-c", "--count"):
                self.__count = int(arg)

        if len(self.__processes) == 0:
            print "Must specify at least one process with -p argument"
            print usage_txt
            sys.exit(2)

        if len(self.__files) > 0:
            if self.__genus is not None or self.__species is not None:
                print "Cannot select genus and species if files are specified"
                print usage_txt
                sys.exit(2)

            self.__selection_criteria = "files"
        else:
            if self.__genus is None and self.__species is None:
                print "Must specify genus and/or species to process"
                print usage_txt
                sys.exit(2)

            self.__selection_criteria = "genus_species"

    @property
    def genus(self):
        return self.__genus

    @property
    def species(self):
        return self.__species

    @property
    def processes(self):
        return self.__processes

    @property
    def files(self):
        return self.__files

    @property
    def selection_criteria(self):
        return self.__selection_criteria

    @property
    def count(self):
        return self.__count

    @staticmethod
    def validate_and_get_processes(arg):
        processes = arg.split(",")

        # TODO: should we validate process names, or is a list like p1,p2,p3 fine?
        return processes
