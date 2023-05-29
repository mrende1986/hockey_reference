import requests
import boto3
import configparser
import psycopg2
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

    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    access_key = parser.get(
                    "aws_boto_credentials",
                    "access_key")
    secret_key = parser.get(
                    "aws_boto_credentials",
                    "secret_key")
    bucket_name = parser.get(
                    "aws_boto_credentials",
                    "bucket_name")

    s3 = boto3.client(
            's3',
            aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    s3_file = local_filename

    s3.upload_file(
        local_filename,
        bucket_name,
        s3_file)
    print("File uploaded")

def s3_to_postgres():

    # Setup connections
    parser = configparser.ConfigParser()
    parser.read("pipeline_local.conf")
    # PostGres
    dbname = parser.get("postgres_config_2", "database")
    user = parser.get("postgres_config_2", "username")
    password = parser.get("postgres_config_2","password")
    host = parser.get("postgres_config_2", "hostname")
    port = parser.get("postgres_config_2", "port")
    # S3 Bucket
    account_id = parser.get("aws_boto_credentials",
                  "account_id")
    iam_role = parser.get("aws_creds", "iam_role")
    bucket_name = parser.get("aws_boto_credentials",
                    "bucket_name")

    # connect to the postgres cluster
    pgs_conn = psycopg2.connect(
            "dbname=" + dbname
            + " user=" + user
            + " password=" + password
            + " host=" + host,
            port = port)

    if pgs_conn is None:
        print("Error connecting to PostGres")
    else:
        print("PostGres connection established!")

    cur = pgs_conn.cursor()

    # Load the file from the S3 bucket
    file_path = ("s3://"
        + bucket_name
        + "/jackhuges-v3.csv")
    role_string = ("arn:aws:iam::"
        + account_id
        + ":role/" + iam_role)

    print(f"S3 File Path is: {file_path}")
    print(f"iam role is: {role_string}")

    # Load the data into PostGres
    df = pd.read_csv(file_path)
    cols = ",".join([str(i) for i in df.columns.tolist()])
    for i, row in df.iterrows():
        sql = "INSERT INTO gamelog (" + cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cur.execute(sql, tuple(row))


    pgs_conn.commit()
    pgs_conn.close()
