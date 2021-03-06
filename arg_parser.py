import sys, getopt


class ArgParser:

    def __init__(self):
        self.__sample_ids = None
        self.__processes = None

    def parse(self):
        usage_txt = 'Usage: -i <ids> -p <processes>'
        try:
            opts, args = getopt.getopt(sys.argv[1:], "hi:p:e:", [])
        except getopt.GetoptError:
            print usage_txt
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                print usage_txt
                sys.exit()
            elif opt == "-i":
                self.__sample_ids = arg.split(",")
            elif opt == "-p":
                self.__processes = arg.split(",")

        if self.__sample_ids is None or len(self.__sample_ids) == 0:
            print "Must specify at least one sample ID with -i argument"
            print usage_txt
            sys.exit(2)

        if self.__processes is None or len(self.__processes) == 0:
            print "Must specify at least one process with -p argument"
            print usage_txt
            sys.exit(2)

    @property
    def processes(self):
        return self.__processes

    @property
    def sample_ids(self):
        return self.__sample_ids

