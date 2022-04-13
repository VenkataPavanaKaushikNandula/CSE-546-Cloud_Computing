import boto3
import json
import os
import base64
import io

class ProcessImage():
    def __init__(self):
        self.s3_input_bucket_name= "cse546group27inputbucket"
        self.s3_output_bucket_name="cse546group27outputbucket"
        self.sqs_queue_name = "CSE546_Group27_SQS"
        self.sqs_queue_url = 'https://queue.amazonaws.com/116117304770/CSE546_Group27_SQS'
        self.sqs_response_queue = "https://sqs.us-east-1.amazonaws.com/116117304770/CSE546_Group27_Response_Queue"
        self.sqs_response_queue_name = "CSE546_Group27_Response_Queue"
        self.download_folder_for_images = "/home/ec2-user/StoreImages"
        self.sqs_service = boto3.resource("sqs")
        self.sqs_client = boto3.client("sqs")
    def fetch_image_from_sqs(self):
        queue = self.sqs_service.get_queue_by_name(QueueName=self.sqs_queue_name)
        messages = queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=20)
        content = ['no_message']
        if messages:
            for each_message in messages:
                content = json.loads(each_message.body)
                each_message.delete()
        else:
            print("No messages in Queue")
        return content

    def fetch_image_image_from_sqs(self,content):
        with open(self.download_folder_for_images+"/" + content[4], "wb") as file_to_save:
            msg1 = content[2].encode('utf-8')
            # print("$$$$$$",str(msg1)[1:])
            decode_image = base64.decodebytes(msg1)
            # print()
            # print(decode_image)
            file_to_save.write(decode_image)
        #with open(self.download_folder_for_images+"/" + content[4]) as f:
        #    self.image_upload(f, content[4])
        file = io.open(self.download_folder_for_images+"/" + content[4], "rb", buffering = 0)
        self.image_upload(io.BytesIO(file.read()), content[4])
        self.process_image(content[4])

    def image_upload(self, file, file_name):
        boto3_session =  boto3.Session()
        s3 = boto3_session.client("s3")
        self.upload_image_to_s3(s3, file, file_name)

        # sqs = boto3_session.client("sqs")
        # self.upload_message_to_sqs_queue(self.encode_image(file), sqs, file_name)
        return "Success"

    def upload_image_to_s3(self, s3_client, file, file_name):
        s3_client.upload_fileobj(file, self.s3_input_bucket_name, str(file_name))
        print("Image uploaded to S3")
        return

    def process_image(self, file_name):
        boto3_session =  boto3.Session()
        s3 = boto3_session.client("s3")
        # self.download_image(s3, file_name)
        downloaded_image = self.download_folder_for_images + "/" + file_name
        image_processed_file = self.download_folder_for_images + "/" + file_name.split(".")[0] + ".txt"

        print("Processing the Image !!!")
        os.system("python3 /home/ec2-user/face_recognition.py " + downloaded_image + " > " + image_processed_file)
        self.upload_result_to_s3(s3, image_processed_file, file_name)

    def download_image(self,s3_client, key_name):
        if not os.path.exists(self.download_folder_for_images):
            os.makedirs(self.download_folder_for_images)
        s3_client.download_file(self.s3_input_bucket_name, key_name, self.download_folder_for_images + "/" + key_name)
        print("Image Downloaded!!!")

    def upload_result_to_s3(self, s3_client, file_path, file_name):
        with open(file_path, "r") as f:
            content = f.readline()
            print("content of file  ", content)
        with open(file_path, "w") as f:
            f.write(file_name + ":" + content)
        s3_client.put_object(Body=content, Bucket=self.s3_output_bucket_name, Key=file_name.replace(".jpg",".txt"))
        self.upload_result_to_sqs_response(file_path, s3_client)

    def upload_result_to_sqs_response(self, file_path, s3_client):
        with open(file_path, "r") as f:
            content = f.readline()
            print(content)
            key, value = content.split(":")
            message = {key : value}
            print("Message: ", message)
            print(value)
            response = self.sqs_client.send_message(QueueUrl=self.sqs_response_queue, MessageBody=json.dumps(message))
            print("Message Upload to Response Queue", response)


if __name__=="__main__":
    process_image = ProcessImage()
    while True:
        content = process_image.fetch_image_from_sqs()
        if content[0] == "process":
            process_image.fetch_image_image_from_sqs(content)
        else:
            break