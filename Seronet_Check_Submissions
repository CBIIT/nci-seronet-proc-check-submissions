import json
import boto3

def lambda_handler(event, context):
    # After file-validator runs it will produce a Result_Message.txt for each submission
    # the creation of this file will cause this function to trigger
    
    s3_client = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    ssm = boto3.client("ssm")
    
    bucket = event["Records"][0]["s3"]["bucket"]["name"]            #bucket of the file, this is the destination bucket
    file_path = event["Records"][0]["s3"]["object"]["key"]          #name of the file, this is Result_Message.tex
    
    # example file_path: 
    # cbc01/2023-04-20-12-59-25/submission_007_Prod_data_for_feinstein20230420_VaccinationProject_Batch9_shippingmanifest.zip/File_Validation_Results/Result_Message.txt
    Unzipped_key = file_path.replace("File_Validation_Results/Result_Message.txt",  "UnZipped_Files/submission.csv")
    sheet_names = []

    try:
        csv_obj = s3_client.get_object(Bucket=bucket, Key=Unzipped_key)
        body = csv_obj['Body']
        csv_string = body.read().decode('utf-8')
        lines = csv_string.splitlines()
        for iterZ in lines:
            split_lines = iterZ.split(',')
            sheet_names.append(split_lines[0])
        sheet_names = sheet_names[7:]
    except Exception as e:
        print(e)
        print("submission.csv does not exist")
############################################################################################################      
## if passed file-validation then move submission according to its submission.csv contents
## if submission.csv is missing will default to the accrual bucket but will be caught in validation email

    if "baseline.csv" in sheet_names:  #submission is data
        dest_folder = "Data_Submissions_Need_To_Validate"
    else:
        dest_folder = "Accrual_Need_To_Validate"
    
    sub_path = file_path.replace("File_Validation_Results/Result_Message.txt", "")
    all_files = s3_client.list_objects_v2(Bucket=bucket, Prefix=sub_path)["Contents"]
    move_files = [i["Key"] for i in all_files]
    
    for curr_file in move_files:
        new_key = dest_folder + "/" + curr_file                     # new destination
        source = {'Bucket': bucket, 'Key': curr_file}               # files to copy
        try:
            s3_resource.meta.client.copy(source, bucket, new_key)
        except Exception as error:
            print('Error Message: {}'.format(error))
