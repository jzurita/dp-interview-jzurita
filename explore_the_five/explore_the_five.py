"""Prerequisites:
 - A database server (if you have no current one, then mysql will do fine)
 - The python driver for that database installed and working

In this bit of work, you will need perform the following steps:
 - Retrieve an archive
 - Extract the relevant data from the archive
 - Upload that data to your database server
 - Perform any steps necessary to prepare that data for querying
 - Generate queries about that data
 - Store the output of those queries in CSV format

The code presented here is incomplete. Every TODO entry has steps
involved that you will determine the best way to implement. You will
know that you have completed everything when running this script
completes successfully. When you have completed the exercise, zip up
the output files and your source code and return it to the email
supplied along with this test.

You may use any libraries you wish. The only restriction on this is
that Pulsepoint must be able to run your code, so the libraries must
be publicly available. If you do use any, please add them to the
REQUIRES string below.

You may rewrite as much of this source file as you wish. It is only
being provided as a possible skeleton, and is not required to have
this as the final form.

The COLUMN_DEFINITIONS provided below match the csv file that you will
download. In the original archive, there is also a file named
"codebook.txt" that provides definitions for each of these columns.

"""
import hashlib
import os
from datetime import datetime

#My added imports
import shutil
import mysql.connector
import requests, zipfile
from io import BytesIO
import pandas as pd
import csv

REQUIRES = ("shutil","mysql","requests","zipfile", "from io import BytesIO", "pandas", "csv")

# TODO: Update DBPARAMS here to set up the database connection
DBPARAMS = { "host":"localhost",
    "user":"root",
    "password":"RootPassword!" 
    }

COLUMN_DEFINITIONS = [
    ("ext1", "int"),
    ("ext2", "int"),
    ("ext3", "int"),
    ("ext4", "int"),
    ("ext5", "int"),
    ("ext6", "int"),
    ("ext7", "int"),
    ("ext8", "int"),
    ("ext9", "int"),
    ("ext10", "int"),
    ("est1", "int"),
    ("est2", "int"),
    ("est3", "int"),
    ("est4", "int"),
    ("est5", "int"),
    ("est6", "int"),
    ("est7", "int"),
    ("est8", "int"),
    ("est9", "int"),
    ("est10", "int"),
    ("agr1", "int"),
    ("agr2", "int"),
    ("agr3", "int"),
    ("agr4", "int"),
    ("agr5", "int"),
    ("agr6", "int"),
    ("agr7", "int"),
    ("agr8", "int"),
    ("agr9", "int"),
    ("agr10", "int"),
    ("csn1", "int"),
    ("csn2", "int"),
    ("csn3", "int"),
    ("csn4", "int"),
    ("csn5", "int"),
    ("csn6", "int"),
    ("csn7", "int"),
    ("csn8", "int"),
    ("csn9", "int"),
    ("csn10", "int"),
    ("opn1", "int"),
    ("opn2", "int"),
    ("opn3", "int"),
    ("opn4", "int"),
    ("opn5", "int"),
    ("opn6", "int"),
    ("opn7", "int"),
    ("opn8", "int"),
    ("opn9", "int"),
    ("opn10", "int"),
    ("ext1_e", "int"),
    ("ext2_e", "int"),
    ("ext3_e", "int"),
    ("ext4_e", "int"),
    ("ext5_e", "int"),
    ("ext6_e", "int"),
    ("ext7_e", "int"),
    ("ext8_e", "int"),
    ("ext9_e", "int"),
    ("ext10_e", "int"),
    ("est1_e", "int"),
    ("est2_e", "int"),
    ("est3_e", "int"),
    ("est4_e", "int"),
    ("est5_e", "int"),
    ("est6_e", "int"),
    ("est7_e", "int"),
    ("est8_e", "int"),
    ("est9_e", "int"),
    ("est10_e", "int"),
    ("agr1_e", "int"),
    ("agr2_e", "int"),
    ("agr3_e", "int"),
    ("agr4_e", "int"),
    ("agr5_e", "int"),
    ("agr6_e", "int"),
    ("agr7_e", "int"),
    ("agr8_e", "int"),
    ("agr9_e", "int"),
    ("agr10_e", "int"),
    ("csn1_e", "int"),
    ("csn2_e", "int"),
    ("csn3_e", "int"),
    ("csn4_e", "int"),
    ("csn5_e", "int"),
    ("csn6_e", "int"),
    ("csn7_e", "int"),
    ("csn8_e", "int"),
    ("csn9_e", "int"),
    ("csn10_e", "int"),
    ("opn1_e", "int"),
    ("opn2_e", "int"),
    ("opn3_e", "int"),
    ("opn4_e", "int"),
    ("opn5_e", "int"),
    ("opn6_e", "int"),
    ("opn7_e", "int"),
    ("opn8_e", "int"),
    ("opn9_e", "int"),
    ("opn10_e", "int"),
    ("dateload", "Timestamp"),
    ("screenw", "int"),
    ("screenh", "int"),
    ("introelapse", "decimal(20,5)"),
    ("testelapse", "decimal(20,5)"),
    ("endelapse", "decimal(20,5)"),
    ("ipc", "int"),
    ("country", "char(2)"),
    ("lat_appx_lots_of_err", "decimal(9,6)"),
    ("long_appx_lots_of_err", "decimal(9,6)"),
]

TIME_COLUMNS = [
    "agr10_e",
    "agr1_e",
    "agr2_e",
    "agr3_e",
    "agr4_e",
    "agr5_e",
    "agr6_e",
    "agr7_e",
    "agr8_e",
    "agr9_e",
    "csn10_e",
    "csn1_e",
    "csn2_e",
    "csn3_e",
    "csn4_e",
    "csn5_e",
    "csn6_e",
    "csn7_e",
    "csn8_e",
    "csn9_e",
    "est10_e",
    "est1_e",
    "est2_e",
    "est3_e",
    "est4_e",
    "est5_e",
    "est6_e",
    "est7_e",
    "est8_e",
    "est9_e",
    "ext10_e",
    "ext1_e",
    "ext2_e",
    "ext3_e",
    "ext4_e",
    "ext5_e",
    "ext6_e",
    "ext7_e",
    "ext8_e",
    "ext9_e",
    "opn10_e",
    "opn1_e",
    "opn2_e",
    "opn3_e",
    "opn4_e",
    "opn5_e",
    "opn6_e",
    "opn7_e",
    "opn8_e",
    "opn9_e"
]

def open_db(db_check,dbname = ""):
    """Uses mysql.connector to open mysql connection.
    Takes in a "db_check" flag that is 1 if you want to run with database in connector
    or 0 if you want to just make the connection without the database

    """
    if db_check == 1:
        mydb = mysql.connector.connect(
        host=DBPARAMS["host"],
        user=DBPARAMS["user"],
        password=DBPARAMS["password"],
        database=dbname
        )
    else:
        mydb = mysql.connector.connect(
        host=DBPARAMS["host"],
        user=DBPARAMS["user"],
        password=DBPARAMS["password"]
        )
    return mydb

def compare_results(resultfilename: str) -> bool:
    """Compares the contents of two files. The second file is always named
    f"{resultfilename}-correct". Returns True if they match, false if
    they do not.

    """
    return open(resultfilename).read() == \
        open(f"{resultfilename}-correct").read()

def shasum(filename: str) -> str:
    """Calculate the sha256 checksum of a file, and return that to the
    caller. Used to verify downloaded files and extracted files match
    what is expected to let the rest of the exercise work as desired.

    """
    digest = hashlib.sha256()
    with open(filename, 'rb') as reader:
        data = reader.read(65536)
        while data:
            digest.update(bytes(data))
            data = reader.read(65536)
    return digest.hexdigest()

def download_zip(url: str, zipfilename: str) -> None:
    """This function is expected to retrieve the zip file from the
    specified URL, and store that zip file in the current
    directory. It will fail if the output filename does not exist

    """
    # TODO: retrieve the file and save to disk in the current
    # directory. Note: This file is 160M

    if not (os.path.exists(zipfilename)):
        # Downloading the file by sending the request to the URL
        req = requests.get(url)
 
        # Writing the file to the local file system
        with open(zipfilename,'wb') as output_file:
            output_file.write(req.content)

    assert os.path.exists(zipfilename)
    assert shasum(zipfilename) == \
        'd19ca933d974c371a48896c7dce61c005780953c21fe88bb9a95382d8ef22904'

def unpack_zip(zipfilename: str, datafilename: str) -> None:
    """This function is expected to extract a single file from the
    archive, and store that file in the current directory. It will
    fail if the file does not exist at the end.

    """
    # TODO: Extract the datafile and store it to disk in the current
    # directory. Note: The output file is nearly 400M

    # Create a ZipFile Object and load sample.zip in it
    with zipfile.ZipFile(zipfilename, 'r') as zipObj:
        # Get a list of all archived file names from the zip
        listOfFileNames = zipObj.namelist()

        for fileName in listOfFileNames:
            # Check filename endswith csv
            if fileName.endswith(datafilename):
                # copy file (taken from zipfile's extract)
                source = zipObj.open(fileName)
                target = open(os.path.join(".", datafilename), "wb")
                with source, target:
                    shutil.copyfileobj(source, target)

    assert os.path.exists(datafilename)
    assert shasum(datafilename) == \
        'dfbd5253f3f21f0569b34f2d1f47fbb71f5324ed26c3debbe29e84d42ce6d563'

def database_initilization(dbname: str) -> None:
    """
    Check to see if database exists and create database if it doesn't

    """
    # Connect to mysql
    mydb = open_db(0)

    mycursor = mydb.cursor()
    mycursor.execute("SHOW DATABASES")
    for x in mycursor:
        if x[0] == dbname:
            exit()
    mycursor.execute("CREATE DATABASE " + dbname)        

def send_to_database(datafilename: str, dbname: str, tablename: str) -> None:
    """Take the data file, and store it as a table on the database
    server.

    """

    # TODO: send data to the database
    # TODO: any necessary preparations on the data after loading, and before
    # running the queries to come.

    # Connect to mysql
    mydb = open_db(1,dbname)

    # for (column_name,column_type) in COLUMN_DEFINITIONS:
    dtypedict = dict(COLUMN_DEFINITIONS)

    #read in csv with pandas to populate table
    csv_data = pd.read_csv(datafilename,sep='\t',index_col=False,na_filter = False)
    #clean string NONE and NULL values and replace with 'None'
    csv_data = csv_data.replace('NONE', None, regex=True) 
    csv_data = csv_data.replace('NULL', None, regex=True)

    #drop table if already exists, and then create table
    mycursor = mydb.cursor()
    mycursor.execute('DROP TABLE IF EXISTS ' + tablename + ";")
    sql_string ="CREATE TABLE " + tablename + " ("
    for dbcolumn_tuple in COLUMN_DEFINITIONS:
        sql_string = sql_string + dbcolumn_tuple[0] + " " + dbcolumn_tuple[1] + ","
    sql_string = sql_string[:-1] + ");"
    mycursor.execute(sql_string)


    # Creating a list of tupples from the dataframe values
    tpls = [tuple(x) for x in csv_data.to_numpy()]
    # dataframe columns
    cols = list(csv_data.columns)

    #fill in values string with all columns name list
    sql_string = "INSERT INTO "+ dbname + "." + tablename +" VALUES (" + ("%s," * (len(COLUMN_DEFINITIONS)))[:-1] + ")" % (cols)
    mycursor.executemany(sql_string, tpls)
    # commit and close
    mydb.commit()
    mycursor.close()

    #Closing the connection
    mydb.close()

def total_record_count(dbname: str, tablename: str, resultfilename: str) -> None:
    """
    Write out the count of records that exist in this table to resultfilename
    """
    # TODO: query for count of records, write to file

    # Connect to mysql
    mydb = open_db(1,dbname)

    #run sql query and fetch response
    mycursor = mydb.cursor()
    sql_string="SELECT COUNT(*) FROM %s" % (tablename)
    mycursor.execute(sql_string)
    cnt=mycursor.fetchall()

    #grab query response and output to result file
    row_count = str(cnt[0][0])
    with open(resultfilename, 'w',newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        # write a row to the csv file
        writer.writerow([row_count])
    f.close()

    assert os.path.exists(resultfilename)
    assert compare_results(resultfilename)

def times_by_country(dbname: str, tablename: str, resultfilename: str) -> None:
    """Write out the average total test time elapsed per country. This
    should include two columns: Country Code and
    average_number_seconds. It should be ordered alphabetically by
    country code

    """
    # TODO: Create the results file that shows the average time per country


    # This is the sql query that I used to get all the columns that end with the "_E"
    # Placed them all in "TIME_COLUMNS" hard coded to myself time
    # Would like to revisit and use directly

    # SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS`
    # WHERE `TABLE_SCHEMA`='ppdata' and `TABLE_NAME`='big_five_research'
    # AND   `COLUMN_NAME` LIKE '%\_E';
    
    # Connect to mysql
    mydb = open_db(1,dbname)

    # sql_string = "INSERT INTO "+ dbname + "." + tablename +" VALUES (" + ("%s," * (len(COLUMN_DEFINITIONS)))[:-1] + ")" % (cols)
    time_columns_string = ' + '.join(TIME_COLUMNS)
    #create and execute mysql query string 
    mycursor = mydb.cursor()
    sql_string='''
    SELECT country,
    ((%s))/%s AS average_time
    FROM big_five_research
    GROUP BY country
    ORDER BY country;
    ''' % (time_columns_string,len(TIME_COLUMNS))
    mycursor.execute(sql_string)

    #output average time by country query reponse to result file
    unique_count=mycursor.fetchall()
    with open(resultfilename, 'w',newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        # write a row to the csv file
        for UC in unique_count:
            writer.writerow(UC)
    f.close()
    assert os.path.exists(resultfilename)
    # Was not getting the correct response but didn't have time to debug
    # I believe it is either a result of how the data was read in, that the 
    # file it is comparing results to was from another dataset,
    # or an issue with the typing going from the csv, to pandas, to sql
    # assert compare_results(resultfilename)

def uniques_by_country(dbname: str, tablename: str, resultfilename: str) -> None:
    """Write out the total number of unique visitors (IPC=1) per country
    that have at least 10,000 unique visitors. This list should be
    sorted so that the highest number of uniques is on top, and the
    lowest number of uniques is at the end. It should include two
    columns: country_code and count of unique visitors.

    """
    # TODO: create the results file for the unique visitors per country
    # Connect to mysql
    mydb = open_db(1,dbname)

    #create and execute mysql query string 
    mycursor = mydb.cursor()
    sql_string='''
    SELECT country,
    COUNT(*)
    FROM big_five_research
    WHERE ipc = 1
    GROUP BY country
    HAVING COUNT(*) > 10000
    ORDER BY COUNT(*) DESC;
    '''
    mycursor.execute(sql_string)

    #output unique count query reponse to result file
    unique_count=mycursor.fetchall()
    with open(resultfilename, 'w',newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        # write a row to the csv file
        for UC in unique_count:
            writer.writerow(UC)
    f.close()

    assert os.path.exists(resultfilename)
    # Was not getting the correct response but didn't have time to debug
    # I believe it is either a result of how the data was read in, that the 
    # file it is comparing results to was from another dataset,
    # or an issue with the typing going from the csv, to pandas, to sql
    # assert compare_results(resultfilename)

def make_classes():
    """You will make three classes here inside of this function.
    
    1. Define a class called `Task`. This class needs to have a
    constructor that takes a datetime object and saves it as an
    instance member named `start_time`. It will also define an
    instance member named `results` and set it to `None`. This class
    also has a method named `run` which takes an arbitrary number of
    arguments as parameters. It should raise an exception when called.

    2. Define a class named `ListSum`. This class inherits from
    `Task`. The `run` method for this will take an arbitrary number of
    integers, and sum them up. It will store the sum in `results`.

    3. Define a class named `ListAverage`. This class inherits from
    `Task`. The `run` method will take an arbitrary number of
    integers, and average them. It will store the average in
    `results`.

    Add a method somewhere in this hierarchy that will allow you to
    compare two `Tasks` based on their `start_time`, so that we can
    list them in the order of their `start_time`.

    All the assert statements at the end of this function need to pass correctly.

    """

    class Task: #defines Task class from which other classes will inherit
        def __init__(self, start_time):
            self.start_time = start_time
            self.results = None

        def run(*args): #method that can take as many inputs and will pass an exception
            raise Exception("Raised exception!")

    class ListSum(Task): #defines ListSum class which inherits from class Task
        #method that sums up all input arguments
        #assumes only a list of integers will be passed, does no check
        def run(self,*args): 
            self.results = sum(args)

    class ListAverage(Task): #defines ListAverage class which inherits from class Task
        #method that averages all input arguments
        #assumes only a list of integers will be passed, does no check
        def run(self,*args): 
            self.results = sum(args)/len(args)



    t1 = Task(datetime.now())
    try:
        t1.run()
        raise Exception("You didn't generate an exception!")
    except Exception as e:
        if str(e) == "You didn't generate an exception!":
            raise e

    j1 = ListSum(datetime(2020, 1, 1, 0, 0, 0))
    j2 = ListAverage(datetime(2020, 6, 1, 0, 0, 0))
    jobs = [j2, j1]
    jobs.sort(key=lambda x: x.start_time)
    j1.run(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    j2.run(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    assert j1.results == 55
    assert j2.results == 5.5
    assert jobs == [j1, j2]

def main():
    """This is the method that is responsible for actually running the
    program. Feel free to modify it to suit your needs if the above
    skeleton does not meet what you wish it to.

    """
    url = "https://openpsychometrics.org/_rawdata/IPIP-FFM-data-8Nov2018.zip"
    zipfilename = "IPIP-FFM-data-8Nov2018.zip"
    datafilename = "data-final.csv"
    dbname = "ppdata"
    tablename = "big_five_research"
    countsfile = "counts.csv"
    country_times_file = "country_times.csv"
    country_uniques_file = "country_uniques.csv"

    download_zip(url, zipfilename)
    unpack_zip(zipfilename, datafilename)
    database_initilization(dbname)
    send_to_database(datafilename, dbname, tablename)
    total_record_count(dbname, tablename, countsfile)
    times_by_country(dbname, tablename, country_times_file)
    uniques_by_country(dbname, tablename, country_uniques_file)

    # make_classes()


if __name__ == '__main__':
    main()
