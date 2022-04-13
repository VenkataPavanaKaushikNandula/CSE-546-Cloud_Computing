from flask import Flask, request, redirect
import json
import boto3
import time
import base64
from werkzeug.utils import secure_filename
import os


boto3_client = boto3.client("s3")
UPLOAD_FOLDER = '/home/ec2-user/StoreImages/'

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


class UploadImage:
    def __init__(self) -> None:
        # Please provide your aws region where s3 bucket and SQS are created.
        self.aws_region = "us-east-one"
        # Please provide your input bucket name. Just the name is enough because it is running in ec2. arn format not required.
        # Example :- self.s3_input_bucket_name= "cse546group27inputbucket"
        self.s3_input_bucket_name= "cse546group27inputbucket"
        # Please provide your output bucket name. Just the name is enough because it is running in ec2. arn format not required.
        self.s3_output_bucket_name="cse546group27outputbucket"
        # Please provide your SQS name. Just the name is enough because it is running in ec2. arn format not required.
        self.sqs_queue_name = "CSE546_Group27_SQS"
        self.sqs_queue_url = 'https://queue.amazonaws.com/116117304770/CSE546_Group27_SQS'


    def image_upload(self, file, file_name):
        boto3_session =  boto3.Session()
        s3 = boto3_session.client("s3")
        # self.upload_image_to_s3(s3, file, file_name)

        sqs = boto3_session.client("sqs")
        self.upload_message_to_sqs_queue(self.encode_image(file), sqs, file_name)
        return "Success"

    def encode_image(self, image):
        with open(image, 'rb') as binary:
            binary_data = binary.read()
            encoded = base64.b64encode(binary_data)
            msg = encoded.decode('utf-8')
        return msg


    def upload_message_to_sqs_queue(self, encoded_bytes, sqs_client, file_name):
        message_attributes = {
        'Title': {
            'DataType': 'String',
            'StringValue': 'Image Name'
        },
        'Author': {
            'DataType': 'String',
            'StringValue': 'Sudarshan Darshin Koushik'
        }
        }
        message_body = json.dumps(["process", self.s3_input_bucket_name, encoded_bytes, "", file_name])
        sqs_client.send_message(QueueUrl=self.sqs_queue_url, MessageAttributes=message_attributes, MessageBody=message_body)
        print("Message Uploaded to Queue")
        return


@app.route('/upload-image', methods=['POST'])
def upload_image():
    aws_object = UploadImage()
    file_name = ""
    for file in request.files.getlist('file'):
        file_name = file.filename
        if file:
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        # if file.filename:
            aws_object.image_upload(UPLOAD_FOLDER+filename, file.filename)
    return "File Uploaded!"
    #return redirect(f"/{file_name}")
my_dict = dict()
sqs_service = boto3.resource("sqs")
@app.route('/<file>', methods=["GET"])
def fetch_output(file):
    global sqs_service
    global my_dict
    if my_dict[file] == "":
    #time.sleep(400)
        queue = sqs_service.get_queue_by_name(QueueName="CSE546_Group27_Response_Queue")
        messages = queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=20)
        if messages:
            for each_message in messages:
                content = json.loads(each_message.body)
                each_message.delete()
                my_dict[list(content.keys())[0]] = list(content.values())[0]
    return my_dict[file]


if __name__=="__main__":
    app.run(host='0.0.0.0',port=5000, debug=True)