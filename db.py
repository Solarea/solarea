import boto3
import mysql.connector

user = "solarea"
password = "solarea1"
host = "solareadb.ccuiq0ahmnzk.us-east-1.rds.amazonaws.com"
database = "solarea"


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

                instance_type = parts[1]
                process_count = get_processes_per_instance(instance_type)

                cursor.execute(
                    "SELECT COUNT(*) FROM ec2_instance_processes WHERE ec2_instance_name=%s",
                    (name,)
                )
                result = cursor.fetchone()

                if process_count < result[0]:
                    raise Exception("Too many records in DB, "
                                    "try deleting ec2_instance_processes records"
                                    " for type '" + instance_type + "'")

                # Add any missing ones
                for x in range(0, process_count - result[0]):
                    cursor.execute(
                        "INSERT ec2_instance_processes (ec2_instance_name, hostname, status) "
                        "VALUES (%s, %s, 'idle')",
                        (name, i.public_dns_name)
                    )

    cnx.commit()
    cursor.close()
    cnx.close()


def get_processes_per_instance(instance_type):
    if instance_type == "genomes":
        return 2

    if instance_type == "meta-genome":
        return 2

    if instance_type == "prokka":
        return 4

    raise Exception("unsupported instance type " + instance_type)


# Returns the total number of slots available for server processing
def get_db_ec2instance_count():
    cnx = connect_to_db()
    cursor = cnx.cursor()

    cursor.execute("SELECT count(*) c FROM ec2_instance_processes")
    result = cursor.fetchone()

    cursor.close()

    return result[0]


# Look at all the running servers and get one that isn't overloaded
def take_next_available_ec2instance_process_slot(process_type, file_info):
    cnx = connect_to_db()

    cursor = cnx.cursor()
    ec2instance_process = None

    cursor.execute("SELECT id, hostname FROM ec2_instance_processes "
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
    cursor.execute("UPDATE ec2_instance_processes SET status=%s, end_time=NOW() WHERE id=%s",
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