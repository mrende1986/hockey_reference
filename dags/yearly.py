import requests
import boto3
import configparser
import pandas as pd

def run_hockey_ref_to_s3():
    result = pd.DataFrame()
    for i in range (2020,2024):
        year = str(i)
        print(year)
        source = requests.get('https://www.hockey-reference.com/players/h/hugheja03/gamelog/'+year+'#gamelog').text
        
        df = pd.read_html(source,header=1)[0]
        df['year'] = year
        result = result.append(df, sort=False)

    result.columns = ["Rk","Date","Game","Age","Tm","HA","Opp","WL","Goal","Assist","PTS","plus_minus","PIM","EVen","PPlay","SHand","GW","EV","PP","SH","S","S_pct","SHFT","TOI","HIT","BLK","FOW","FOL","FO_pct","year"]
    result = result[result.Rk != 'Rk']

    # save dataframe to local drive
    local_filename = 'jackhuges-v3.csv'
    result.to_csv(local_filename, index=False)
    print("CSV Saved")

    s3 = boto3.client(
            's3',
            aws_access_key_id="", aws_secret_access_key="")
    print("S3 connection setup")
    s3_file = local_filename
    bucket_name = ""


    s3.upload_file(
        local_filename,
        bucket_name,
        s3_file)
    print("File uploaded")
