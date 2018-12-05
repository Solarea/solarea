from processor import *
from arg_parser import *

arg_parser = ArgParser()
arg_parser.parse()

# Updates the database with all EC2 instances
update_ec2_instances_in_db()

# Updates all files that need to be processed
#update_files_in_db_from_excel()

# Start processing data
start_processes(arg_parser)
