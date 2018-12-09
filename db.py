import boto3
import mysql.connector


class DatabaseHelpers:

    def __init__(self, config):
        self.__config = config
        self.__cnx = self.connect_to_db()

    def update_ec2_instances_in_db(self):
        ec2 = boto3.resource('ec2')

        print "Updating EC2 instances in database"

        print " - connecting to DB"
        cursor = self.__cnx.cursor()

        for i in ec2.instances.all():
            if i.tags is not None:
                name = i.tags[0]['Value']
                parts = name.split("-")

                if name.startswith("node-") and len(parts) == 3 and i.state["Name"] != "terminated":

                    instance_type = parts[1]
                    process_count = self.get_processes_per_instance(instance_type)

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
                            "INSERT ec2_instance_processes (ec2_instance_name, ec2_instance_type, hostname, status) "
                            "VALUES (%s, %s, %s, 'idle')",
                            (name, instance_type, i.public_dns_name)
                        )

        self.__cnx.commit()
        cursor.close()

    @staticmethod
    def get_processes_per_instance(instance_type):
        if instance_type == "genomes":
            return 2

        if instance_type == "meta.genomes":
            return 3

        raise Exception("unsupported instance type " + instance_type)

    # Returns the total number of slots available for server processing
    def get_db_ec2instance_count(self):
        cursor = self.__cnx.cursor()

        cursor.execute("SELECT count(*) c FROM ec2_instance_processes")
        result = cursor.fetchone()

        cursor.close()
        return result[0]

    def take_next_available_ec2instance_process_slot(self, process_type):
        cursor = self.__cnx.cursor()
        process_slot = None

        cursor.execute("SELECT e.id, e.hostname "
                       "FROM ec2_instance_processes e "
                       "JOIN process_ec2_instance_map p ON e.ec2_instance_type=p.ec2_instance_type "
                       "WHERE p.process=%s AND status='idle' "
                       "LIMIT 1",
                       (process_type,))
        result = cursor.fetchone()

        if result is not None:
            process_slot = {"id": result[0], "hostname": result[1], "process_type": process_type}
            cursor.execute("UPDATE ec2_instance_processes "
                           "SET status='running' "
                           "WHERE id=%s",
                           (result[0],))
            self.__cnx.commit()

        cursor.close()
        return process_slot

    # Marks the given EC2 instance with the specified status
    def mark_ec2instance_process_status(self, process_slot, status):
        cursor = self.__cnx.cursor()

        cursor.execute("UPDATE ec2_instance_processes SET status=%s WHERE id=%s",
                       (status, process_slot["id"]))
        self.__cnx.commit()
        cursor.close()

    def start_sample_processing(self, sample_id, process, command):
        cursor = self.__cnx.cursor()

        cursor.execute("INSERT sample_processing (sample_id, process, command, start, status) "
                       "VALUES (%s, %s, %s, NOW(), 'running')",
                       (sample_id, process, command))

        last_row_id = cursor.lastrowid
        self.__cnx.commit()
        cursor.close()
        return last_row_id

    def mark_sample_status(self, row_id, status):
        self.__cnx = self.connect_to_db()
        cursor = self.__cnx.cursor()
        cursor.execute("UPDATE sample_processing SET status=%s, end=NOW() WHERE ID=%s",
                       (status, row_id))

        self.__cnx.commit()
        cursor.close()

    # Connects to the database
    def connect_to_db(self):
        return mysql.connector.connect(
            user=self.__config.db_user,
            password=self.__config.db_password,
            host=self.__config.db_host,
            database=self.__config.db_name)