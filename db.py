import boto3
import mysql.connector
import pandas
from file_info import FileInfo
from itertools import ifilterfalse

user = "solarea"
password = "solarea1"
host = "solareadb.ccuiq0ahmnzk.us-east-1.rds.amazonaws.com"
database = "solarea"
spreadsheet = "/Users/tweissin/OneDrive/Documents/solarea/Solarea_metadata_samples 2018.5.30 brief.xlsx"
sheet_name = "strains"
column_name = "Isolate record"


# Reads all EC2 instances in AWS and stores them in the "ec2_instances" table
def update_ec2_instances_in_db():
    ec2 = boto3.resource('ec2')

    print "Updating EC2 instances in database"

    print " - connecting to DB"
    cnx = connect_to_db()
    cursor = cnx.cursor()

    for i in ec2.instances.all():
        if i.tags is not None:
            name = i.tags[0]['Value']
            parts = name.split("-")
            if name.startswith("node-") and len(parts) == 3:
                process_type = parts[1]
                ec2instance = {
                    'hostname': i.public_dns_name,
                    'instance_type': i.instance_type,
                    'process_type': process_type
                }

                cursor.execute(
                    "SELECT id, COUNT(*) FROM ec2instances WHERE hostname=%s",
                    (i.public_dns_name,)
                )
                result = cursor.fetchone()

                if result[0] == 0:
                    insert_ec2_instance(cursor, ec2instance)
                else:
                    print " - EC2 instance already exists: " + name
            else:
                print " - Skipping EC2 instance (name is not in proper format): " + name

    cnx.commit()
    cursor.close()
    cnx.close()


def insert_ec2_instance(cursor, ec2instance):
    print(" - Inserting " + str(ec2instance['hostname']))
    cursor.execute(
        "INSERT INTO ec2instances (hostname, instance_type, process_type, status) VALUES (%s, %s, %s, %s)",
        (ec2instance['hostname'], ec2instance['instance_type'], ec2instance['process_type'], 'available')
    )


# Returns all hostnames in the DB
def get_db_hostnames(cnx):

    hostnames = []
    cursor = cnx.cursor()

    cursor.execute("SELECT hostname FROM ec2instances")
    result = cursor.fetchall()

    for hostname in result:
        hostnames.append(str(hostname[0]))

    cursor.close()

    return hostnames


# Returns the total number of slots available for server processing
def get_db_ec2instance_count():
    cnx = connect_to_db()
    cursor = cnx.cursor()

    cursor.execute("SELECT count(*) c FROM ec2instance_processes")
    result = cursor.fetchone()

    cursor.close()

    return result[0]


def get_files_to_process(arg_parser):
    print "Getting files to process"

    file_infos = []

    if arg_parser.selection_criteria == "files":
        # selection_criteria is files.
        cnx = connect_to_db()
        cursor = cnx.cursor()

        format_strings = ','.join(['%s'] * len(arg_parser.files))
        cursor.execute("SELECT isolate_record, genus, species FROM files WHERE isolate_record IN (%s)" % format_strings,
                       tuple(arg_parser.files))

        # get all that match with the files passed in
        for row in cursor.fetchall():
            file_infos.append(FileInfo(row[0], row[1], row[2]))

        # this gets all processes that are running or have completed the specified files already
        cursor.execute("SELECT f.isolate_record FROM ec2instance_processes sp "
                       "JOIN files f ON f.id=sp.file_id "
                       "WHERE f.isolate_record IN (%s)" % format_strings,
                       tuple(arg_parser.files))

        # exclude files whose ec2instance_processes are not already running or complete
        for row in cursor.fetchall():
            file_infos[:] = ifilterfalse(lambda fi: fi.isolate_record == row[0], file_infos)

        cursor.close()

        # return just the files to process

    elif arg_parser.selection_criteria == "genus_species":
        # selection_criteria is genus_species:
        # return files only where the genus and species match the input
        # exclude files whose ec2instance_processes are not already running or complete
        print "genus and/or species criteria"

    else:
        print "invalid selection_criteria: " + arg_parser.selection_criteria

    # Limit based on count
    if arg_parser.count is not None:
        return file_infos[0:arg_parser.count]

    return file_infos


def print_s3_buckets():

    s3 = boto3.resource('s3')

    for bucket in s3.buckets.all():
        print(bucket.name)


# Takes the spreadsheet as input and updates the files DB table
def update_files_in_db_from_excel():

    print "Update files database with data from Excel"

    print " - reading Excel spreadsheet " + spreadsheet
    df = pandas.read_excel(spreadsheet, sheet_name=sheet_name)

    cnx = connect_to_db()

    print " - reading existing db files"
    db_file_infos = get_db_files(cnx)
    cursor = cnx.cursor()

    db_isolate_records = list(map(lambda x: x.isolate_record, db_file_infos))

    for row in df.iterrows():
        isolate_record = row[1][0]
        if isolate_record not in db_isolate_records:
            print(" - Inserting " + isolate_record)
            if isolate_record == "SBI0013":
                print "bad stuff here"

            genus = row[1].Genus
            species = row[1].species

            if not isinstance(genus, basestring):
                genus = ""

            if not isinstance(species, basestring):
                species = ""

            cursor.execute("INSERT INTO files (isolate_record, genus, species) VALUES (%s, %s, %s)",
                           (isolate_record, genus, species))

    cnx.commit()
    cursor.close()
    cnx.close()


# Returns all the files currently in the DB
def get_db_files(cnx):

    file_infos = []
    cursor = cnx.cursor()

    cursor.execute("SELECT isolate_record, genus, species FROM files")
    result = cursor.fetchall()

    for f in result:
        file_info = FileInfo(str(f[0]), str(f[1]), str(f[2]))

        print "found isolate_record " + file_info.isolate_record
        file_infos.append(file_info)

    cursor.close()

    return file_infos


# Returns the next file that hasn't been processed, and marks as in progress.
def take_next_file_to_process():
    cnx = connect_to_db()

    cursor = cnx.cursor()

    cursor.execute("SELECT id, input_file FROM files WHERE status='not_processed' LIMIT 1")
    result = cursor.fetchone()

    if result is not None:
        cursor.execute("UPDATE files SET status='in_progress', start_time=NOW() WHERE id='" + str(result[0]) + "'")
        cnx.commit()

    cursor.close()

    return result


# Look at all the running servers and get one that isn't overloaded
def take_next_available_ec2instance_process_slot(process_type, file_info):
    cnx = connect_to_db()

    cursor = cnx.cursor()
    ec2instance_process = None

    # cursor.execute("SELECT s.id, sp.id spid, sp.status, sp.process_type, COUNT(*) c, s.hostname "
    #                "FROM ec2instance_processes sp "
    #                "JOIN ec2instances s ON s.id=sp.ec2instance_id "
    #                "WHERE sp.status='running' "
    #                "AND sp.process_type=%s"
    #                "GROUP BY s.id, sp.status, sp.process_type "
    #                "ORDER BY c DESC",
    #                (process_type,))

    cursor.execute("SELECT id, hostname FROM ec2instances "
                   "WHERE sp.process_type=%s "
                   "LIMIT 1",
                   (process_type,))
    result = cursor.fetchone()
    if result is not None:
        ec2instance_id = result[0]

        cursor.execute("INSERT ec2instance_processes (ec2instance_id, process_type, file_id, status, start_time)"
                       " VALUES (%s, %s, %s, 'running', NOW())",
                       (ec2instance_id, process_type, file_info.id))
        ec2instance_process = [cursor.lastrowid]
        cnx.commit()

    cursor.close()

    return ec2instance_process


# Marks the given EC2 instance with the specified status
def mark_ec2instance_process_status(ec2instance_process, status):
    cnx = connect_to_db()

    cursor = cnx.cursor()

    spid = str(ec2instance_process[1])
    cursor.execute("UPDATE ec2instance_processes SET status=%s, end_time=NOW() WHERE id=%s",
                   (status, str(spid)))
    cnx.commit()

    cursor.close()


# Connects to the database
def connect_to_db():
    return mysql.connector.connect(
        user=user,
        password=password,
        host=host,
        database=database)