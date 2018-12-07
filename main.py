from processor import *
from arg_parser import *

arg_parser = ArgParser()
arg_parser.parse()

# Updates the database with all EC2 instances
update_ec2_instances_in_db()

# Start processing data
start_processes(arg_parser)
