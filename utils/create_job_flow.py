import sys
import pickle
sys.path.append('/home/ubuntu/UCSD_BigData/utils')
from find_waiting_flow import *
from AWS_keypair_management import *

def create_job_flow():
    Creds= pickle.load(open('/home/ubuntu/Vault/Creds.pkl','rb'))
    pair=Creds['mrjob']
    key_id=pair['key_id']
    secret_key=pair['secret_key']
    ID=pair['ID']
    return find_waiting_flow(key_id,secret_key)
