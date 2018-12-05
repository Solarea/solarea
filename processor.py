from db import *
import paramiko
import time
import threading
from concurrent.futures import *
import socket


# Starts processing the files
def start_processes(arg_parser):

    # Get total number of servers.  Our thread pool has to be >= server count or else we're not maximizing usage.
    thread_pool_size = get_db_ec2instance_count()

    file_infos = get_files_to_process(arg_parser)

    # This is used to manage spinning off work as a thread
    executor = ThreadPoolExecutor(max_workers=thread_pool_size)
    for file_info in file_infos:

        for process_type in arg_parser.processes:

            # Get a server to do work
            print threading.current_thread().name + ": waiting for available server... to process " \
                  + file_info.isolate_record
            ec2instance_process = wait_for_and_get_ec2instance_process_slot(process_type, file_info)

            # Submit the work to a thread pool
            executor.submit(process_file_on_ec2instance, file_info, ec2instance_process)

    print threading.current_thread().name + ": waiting on threads"
    executor.shutdown()
    exit(0)


# Waits for a server to be available
def wait_for_and_get_ec2instance_process_slot(process_type, file_info):

    while True:
        ec2instance_process = take_next_available_ec2instance_process_slot(process_type, file_info)
        if ec2instance_process is not None:
            hostname = ec2instance_process[0]

            try:
                # Try to make a connection to this server
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

                print "connecting to " + hostname
                ssh.connect(hostname, 22,
                            username="ec2-user",
                            key_filename="/Users/tweissin/.ssh/tonyweis/tonyweissinger.pem",
                            timeout=10)
                return ec2instance_process[0], ec2instance_process[1], ssh
            except (paramiko.BadHostKeyException, paramiko.AuthenticationException,
                    paramiko.SSHException, socket.error) as e:

                # Failed to connect, mark the server in an error state
                print "error connecting to " + hostname + ", marking as error status: " + str(e)
                # TODO
                #mark_server_status(server, "error")

        # Keep waiting for an available server
        print "No available server, sleeping."
        time.sleep(5)


# Processes the specified file on the given server
def process_file_on_ec2instance(file_info, ec2instance_process):
    input_file = file_info[1]
    hostname = ec2instance_process[0]
    ssh = ec2instance_process[2]

    command = "echo Processing " + input_file + " on " + hostname + " >> ~/log.txt"

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
    mark_ec2instance_process_status(ec2instance_process, "complete")

