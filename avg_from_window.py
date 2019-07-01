'''
Author: Michael Lukiman
Dealing with timeseries data, this code gets rows where the date is in the given window. Then, it takes the average of the column(s) given.
'''
import csv, pandas as pd, sys
import datetime as dt
from typing import List

def filterDate(csvpath:str, newcsv:str, name_of_date_col:str, start_date:str, end_date:str):
    '''
    Assuming a 'date' column, only filter rows within a set of dates.
    '''
    chunksize = 20000
    i=0
    writeHeader = True
    for chunk in pd.read_csv(csvpath, chunksize=chunksize):
        print("Chunk",i,'\n---')
        chunk[name_of_date_col] = pd.to_datetime(chunk[name_of_date_col]) # Convert the date string column to datetime objects. Will make it easier to select range. 
        mask = (chunk[name_of_date_col] >= start_date) & (chunk[name_of_date_col] <= end_date) # Make a new mask that will only include certain dates.
        masked_chunk = chunk.loc[mask]
        masked_chunk.to_csv(newcsv, mode='a+', index=False, header=writeHeader)
        i+=1
        writeHeader = False

def findGeometricAvg(csvpath: str, columnName:str):
    chunksize = 20000
    total = 0
    count = 0
    i=0
    for chunk in pd.read_csv(csvpath, chunksize=chunksize):
        print("Chunk",i,'\n---')
        i+=1
        casted_chunk = pd.to_numeric(chunk[columnName], downcast='float',errors='ignore')
        print(casted_chunk)
        total += casted_chunk.sum()
        count += len(chunk)
    return total / count

if __name__ == "__main__":
    # Initialize some things.
    avgs = {} # This dict will hold the averages of specific columns.
    meds = {} # This dict will hold the medians of specific columns.
    args = [None, None] # This will hold the arguments given to the program.
    if len(sys.argv) < len(args) + 1: 
        print('Please input the filename and the comma separated list of columns you want with no spaces i.e. feature_x,feature_y,feature_z. One of these should be \'date\'')

    # Process arguments and the csv.
    data = sys.argv[1]
    quantified_features = sys.argv[2].split(',')
    days_before = int(sys.argv[3])

    # Creates file only with desired dates. Will make new file so don't have to keep in RAM.
    # end_date = dt.date.today() # Now, today.
    # end_str = str(end_date)
    now = dt.datetime.now().strftime("%Y%m%d%H%M%S")
    # start_str = str(end_date - dt.timedelta(days=days_before)) # N days before today.
    end_str = '2018-11-26'
    start_str = '2018-11-20'
    date_pared = f"{start_str}_TO_{end_str}_CREATED-{now}_" + sys.argv[1].split('/')[-1]
    filterDate(data,date_pared,'Date',start_str,end_str) 

    for feature in quantified_features: 
        avgs[feature] = findGeometricAvg(date_pared, feature)

    print('-------------------')
    [print(f'AVG for {key}: {val}') for key, val in avgs.items()]
