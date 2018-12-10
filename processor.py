import paramiko
import time
import threading
from concurrent.futures import *
import socket


class Processor:

    def __init__(self, arg_parser, db, config):
        self.__config = config
        self.__arg_parser = arg_parser
        self.__db = db

    # Starts processing the files
    def start_processes(self):

        # Get total number of servers.  Our thread pool has to be >= server count or else we're not maximizing usage.
        thread_pool_size = self.__db.get_db_ec2instance_count()

        # This is used to manage spinning off work as a thread
        executor = ThreadPoolExecutor(max_workers=thread_pool_size)
        for sample_id in self.__arg_parser.sample_ids:

            for process_type in self.__arg_parser.processes:

                # Get a server to do work
                print threading.current_thread().name + ": waiting for available server... to process " + sample_id
                process_slot = self.wait_for_and_get_ec2instance_process_slot(process_type)

                # Submit the work to a thread pool
                executor.submit(self.process_file_on_ec2instance, sample_id, process_slot)

        print threading.current_thread().name + ": waiting on threads"
        executor.shutdown()
        exit(0)

    # Waits for a server to be available
    def wait_for_and_get_ec2instance_process_slot(self, process_type):

        while True:
            process_slot = self.__db.take_next_available_ec2instance_process_slot(process_type)
            if process_slot is not None:
                hostname = process_slot["hostname"]

                try:
                    # Try to make a connection to this server
                    ssh = paramiko.SSHClient()
                    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

                    print "connecting to " + hostname
                    ssh.connect(process_slot["hostname"], 22,
                                username="ec2-user",
                                key_filename=self.__config.ssh_key_file,
                                timeout=10)
                    process_slot["ssh"] = ssh
                    return process_slot
                except (paramiko.BadHostKeyException, paramiko.AuthenticationException,
                        paramiko.SSHException, socket.error) as e:

                    # Failed to connect, mark the server in an error state
                    print "error connecting to " + hostname + ", marking as error status: " + str(e)
                    self.__db.mark_ec2instance_process_status(process_slot, "error")

            # Keep waiting for an available server
            print "No available server, sleeping."
            time.sleep(5)

    # Processes the specified file on the given server
    def process_file_on_ec2instance(self, sample_id, process_slot):
        ssh = process_slot["ssh"]

        command = "echo Processing " + sample_id + " on " + process_slot["hostname"] + " >> ~/log.txt"
        row_id = self.__db.start_sample_processing(sample_id, process=process_slot["process_type"], command=command)

        print threading.current_thread().name + ": Executing " + command

        # This does the actual work!!
        stdin, stdout, stderr = ssh.exec_command(command)

        data = stdout.readlines()
        print data

        # Simulate a long running task
        print threading.current_thread().name + ": Execution done, sleeping a bit..."
        time.sleep(5)
        print threading.current_thread().name + ": Execution done, done sleeping"

        # When done, mark the process as done
        self.__db.mark_ec2instance_process_status(process_slot, "idle")
        self.__db.mark_sample_status(row_id, "completed")

