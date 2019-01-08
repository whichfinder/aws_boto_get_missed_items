import boto3
import datetime
import logging
import os

logging.getLogger().setLevel(logging.INFO)

s3 = boto3.resource('s3')
bucket = s3.Bucket(os.getenv('BUCKET'))
service_url = os.getenv('SERVICE_URL')
yesterday_date = datetime.datetime.now() - datetime.timedelta(days=1)

def main():
	pathes = get_path_list()
	check_items_in_service(pathes)

def get_path_list():
	path_list = []
	for b in bucket.objects.filter(Prefix='products/{}/{}/{}/'.format(yesterday_date.year, yesterday_date.month, yesterday_date.day)):
		if b.key.endswith('productInfo.json'):
			obj = s3.Object(bucket.name, b.key)
			body = json.loads(obj.get()['Body'].read())
			path_list.append(body['tiles'][0]['path'])
	return path_list

def check_items_in_service(items_list):
    absent_obj_dict = {}
    for i in items_list:
        req = requests.post(service_url, json={"search": {"awsPath": '"\{}\"'.format(i) }}, verify=False)
        if req.json()['meta']['found'] == 0:
                absent_obj_dict[req.json()['results'][0]['sceneID']] = i
                logging.warning('absent obj - {}'.format(i))
        else:
            continue
    if len(absent_obj_dict) > 0:
        logging.warning('missed objects - {}, missed path - {}'.format(absent_obj_dict.keys(), absent_obj_dict.values()))
    else:
        logging.info('all ok')
