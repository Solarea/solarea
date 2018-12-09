from processor import *
from arg_parser import *
from config import Config
from db import DatabaseHelpers

arg_parser = ArgParser()
arg_parser.parse()

config = Config()
db = DatabaseHelpers(config)
processor = Processor(arg_parser, db, config)

# Updates the database with all EC2 instances
db.update_ec2_instances_in_db()

# Start processing data
processor.start_processes()
