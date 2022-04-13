import boto3
import json
import paramiko
import threading
from time import sleep
import time

class Controller():
    def __init__(self):
        self.master_instance_id = "i-004ec846439426020"
        self.list_of_instance_ids = list()
        self.count_of_insances = 0
        self.sqs_queue_url = 'https://queue.amazonaws.com/116117304770/CSE546_Group27_SQS'
        self.sqs_service = boto3.resource("sqs")
        self.sqs_response_queue_name = "CSE546_Group27_Response_Queue"
        self.s3_output_bucket_name="cse546group27outputbucket"
        self.list_of_threads = list()
        self.list_of_processing_instances = list()

    def start_instances(self, no_of_instances):
        for i in range(no_of_instances):
            ec2_client = boto3.client('ec2')
            start_instance = ec2_client.run_instances(
                BlockDeviceMappings=self.ec2_instance_config(),
                ImageId='ami-098ce2ff94234dba9',
                InstanceType='t2.micro',
                KeyName='CSE546_SSH_Access',
                MinCount=1,
                MaxCount=1,
                Monitoring={
                    'Enabled': False
                },
                SecurityGroupIds=[
                    "sg-0282b800556f86195"
                ],
            )
            instance = start_instance["Instances"][0]
            try:
                ec2_client.create_tags(Resources=[instance["InstanceId"]], Tags=[{'Key':'Name', 'Value':'app_tier '+str(i)}])
            except:
                print("Not Tagged ! Instance might be terminated :", instance)
            print(start_instance)

    def ec2_instance_config(self):
        config = [
            {
                'DeviceName': '/dev/xvda',
                'Ebs': {

                    'DeleteOnTermination': True,
                    'VolumeSize': 16,
                    'VolumeType': 'gp2'
                },
            },
        ]
        return config

    def get_count_of_running_and_stopped_instances(self, ec2_client):
        running_instances = self.get_list_of_running_instances(ec2_client)
        stopped_instances = self.get_list_of_stopped_instances(ec2_client)
        return len(running_instances), len(stopped_instances)

    def get_list_of_running_instances(self, ec2_client):
        running_instances = ec2_client.instances.filter(
            Filters=[
                {
                    'Name': 'instance-state-name',
                    'Values': ["running", "pending"]
                }
            ]
        )
        running_instances = [i.id for i in running_instances if i.id != self.master_instance_id]
        return running_instances

    def get_list_of_stopped_instances(self, ec2_client):
        stopped_instances = ec2_client.instances.filter(
            Filters=[
                {
                    'Name': 'instance-state-name',
                    'Values': ["stopped", "stopping"]
                }
            ]
        )
        stopped_instances = [i.id for i in stopped_instances]
        return stopped_instances

    def get_queue_length(self, sqs_client):
        queue = sqs_client.get_queue_attributes(QueueUrl=self.sqs_queue_url, AttributeNames=['ApproximateNumberOfMessages',])
        return int(queue['Attributes']['ApproximateNumberOfMessages'])

    def upload_to_s3_from_sqs_response(self):
        s3_client = boto3.client("s3")
        while True:
            queue = self.sqs_service.get_queue_by_name(QueueName=self.sqs_response_queue_name)
            messages = queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=20)
            if messages:
                for m in messages:
                    content = json.loads(m.body)
                    m.delete()
                    print("content var", content)
                    print("content", list(content.values())[0])
                    s3_client.put_object(Body=list(content.values())[0], Bucket=self.s3_output_bucket_name, Key=list(content.keys())[0].split(".")[0]+".txt")
            else:
                break

    def process_image_in_ec2(self, ec2_client, instance_id):
        key = paramiko.RSAKey.from_private_key_file('/home/ec2-user/IAAS/CSE546_SSH_Access.pem')
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        instance = [i for i in ec2_client.instances.filter(InstanceIds=[instance_id])][0]
        while True:
            try:
                client.connect(hostname=instance.public_ip_address, username="ec2-user", pkey=key, timeout=30)
                print("Starting to Process")
                client.exec_command('python3 /home/ec2-user/process_image.py')
                print("Processed!!")
                client.close()
                break
            except Exception as e:
                print("Reattempting to connect "+str(e))
                sleep(10)

    def spin_up_instances(self):
        ec2_client = boto3.resource("ec2")
        sqs_client = boto3.client("sqs")
        max_instances = 19


        while True:
            print("*"*15, "Start of Loop", "*"*15)
            no_of_running_instances , no_of_stopped_instances = self.get_count_of_running_and_stopped_instances(ec2_client)
            messages_in_queue = self.get_queue_length(sqs_client)
            print("Messages in Queue: ", messages_in_queue)
            print("Running Instances: ", no_of_running_instances)
            print("Stopped Instances: ", no_of_stopped_instances)
            stopped_instances = self.get_list_of_stopped_instances(ec2_client)

            if messages_in_queue > no_of_stopped_instances:
                no_of_new_instances_to_start = min(max_instances - (no_of_running_instances + no_of_stopped_instances), messages_in_queue - no_of_stopped_instances)
                if no_of_new_instances_to_start > 0:
                    print(f"Starting {no_of_new_instances_to_start} new instances")
                    self.start_instances(no_of_new_instances_to_start)
                if len(stopped_instances) > 0:
                    print(f"Starting {stopped_instances} stopped instances")
                    ec2_client.instances.filter(InstanceIds=stopped_instances).start()
                time.sleep(75)

            else:
                no_of_instances_to_start = min(no_of_stopped_instances, messages_in_queue - (no_of_running_instances - len(self.list_of_processing_instances)))
                if no_of_instances_to_start > 0:
                    print(f"Messages are less, Starting {no_of_instances_to_start} instances")
                    ec2_client.instances.filter(InstanceIds=stopped_instances[:no_of_instances_to_start]).start()
                    time.sleep(60)

            self.execute_each_instance(ec2_client)

            idle_instances = list()
            for id in self.get_list_of_running_instances(ec2_client):
                if id not in self.list_of_processing_instances:
                    idle_instances.append(id)

            if len(idle_instances) > 0:
                print("Stopping Idle Instances: ", idle_instances)
                ec2_client.instances.filter(InstanceIds=idle_instances).stop()
                time.sleep(60)

            if self.get_queue_length(sqs_client) == 0:
                #self.upload_to_s3_from_sqs_response()
                time.sleep(20)
            else:
                time.sleep(30)

    def execute_each_instance(self, ec2_client):
        for instance_id in self.get_list_of_running_instances(ec2_client):
            if instance_id not in self.list_of_processing_instances:
                thread = threading.Thread(name=instance_id, target=self.process_image_in_ec2, args=(ec2_client, instance_id))
                self.list_of_threads.append(thread)
                self.list_of_processing_instances.append(instance_id)
                thread.start()
        new_thread_list = []
        for each_thread in self.list_of_threads:
            if not each_thread.is_alive():
                self.list_of_processing_instances.remove(each_thread.getName())
            else:
                new_thread_list.append(each_thread)
        self.list_of_threads = new_thread_list


if __name__=="__main__":
    controller = Controller()
    controller.spin_up_instances()