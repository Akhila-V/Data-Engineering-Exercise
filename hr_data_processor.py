from util import connect_to_postgres, drop_employee_tables
import pandas as pd
import os
from datetime import date, datetime, timedelta

# Create stage table to dump data as is from the CSV files
# All the columns are 
employee_stg_ddl = """
create table if not exists public.employee_stg
(
snapshot_date varchar(10),
employee_number varchar(10),
status varchar(10),
first_name varchar(50),
last_name varchar(50),
gender varchar(10),
email varchar(50),
phone_number varchar(15),
salary varchar(10),
termination_date varchar(10)
);

"""

# Create employee table which consists of current employee snapshot

employee_ddl = """
create table if not exists public.employee
(
snapshot_date date,
employee_number int PRIMARY KEY,
status varchar(10),
first_name varchar(50),
last_name varchar(50),
gender varchar(10),
email varchar(50),
phone_number varchar(15),
salary bigint,
termination_date date,
created_date timestamp,
updated_date timestamp

);

"""

#Create employee_hist table to record the changes

employee_hist_ddl = """
create table if not exists public.employee_hist
(
snapshot_date date,
employee_number int,
status varchar(10),
first_name varchar(50),
last_name varchar(50),
gender varchar(10),
email varchar(50),
phone_number varchar(15),
salary bigint,
termination_date date,
created_date timestamp,
updated_date timestamp,
change_captured_date date,
PRIMARY KEY (employee_number, updated_date)
);
"""

# In the employee_hist table, insert those only the old records from employee table which have been changed in the employee_stg table 

insert_employee_hist = """
with latest_employees as (
select s.*
from public.employee_stg s
where s.snapshot_date = '{curr_date}'
)

, modified_employees as (
select e.*
from latest_employees s
right join public.employee e 
on cast(s.employee_number as int) = e.employee_number 
where s.first_name != e.first_name or s.last_name != e.last_name or lower(s.email) != e.email 
or s.phone_number != e.phone_number
or cast(s.salary as bigint) != e.salary or cast(s.termination_date as date) != e.termination_date
or trim(s.status) != e.status or trim(s.gender) != e.gender
)
insert into public.employee_hist
select *, current_date as change_captured_date from modified_employees
;
"""

#In the employee table, update only those employee records which have been modified in the staging table for that snapshot date
#In this case, the created_date remains the same but the updated_date is the current timestamp

update_employee = """
update employee set 
snapshot_date = cast(s.snapshot_date  as date),
status = trim(s.status),
first_name = trim(s.first_name),
last_name = trim(s.last_name),
gender = trim(s.gender),
email = lower(trim(s.email)),
phone_number = trim(s.phone_number),
salary = cast(s.salary as bigint),
termination_date = cast(s.termination_date as date),
updated_date = current_timestamp
from (
with latest_employees as (
select s.*
from public.employee_stg s
where s.snapshot_date = '{curr_date}'
)
select s.* 
from latest_employees s
left join public.employee e 
on cast(s.employee_number as int) = e.employee_number 
where s.first_name != e.first_name or s.last_name != e.last_name or lower(s.email) != e.email 
or s.phone_number != e.phone_number
or cast(s.salary as bigint) != e.salary or cast(s.termination_date as date) != e.termination_date
or trim(s.status) != e.status or trim(s.gender) != e.gender
) s where cast(s.employee_number as int) = employee.employee_number

"""

#Delete the employees from the employee table which are not present in the employee staging table for that snapshot date

delete_employee = """
delete from employee 
where employee_number not in (
select cast(employee_number as int)
from public.employee_stg 
where snapshot_date = '{curr_date}'
);

"""

#Insert all new employees from the employee staging table into the employee table for that snapshot date
#In this case the created_date and updated_date are the current timestamp
#Also, perform necessary transformations on the data before loading into employee table

insert_employee = """
insert into public.employee
with latest_employees as (
select *
from public.employee_stg s
where s.snapshot_date = '{curr_date}'
)
select 
cast(s.snapshot_date  as date),
cast(s.employee_number as int),
trim(s.status),
trim(s.first_name),
trim(s.last_name),
trim(s.gender),
lower(trim(s.email)),
trim(s.phone_number),
cast(s.salary as bigint),
cast(s.termination_date as date), 
current_timestamp as created_date, 
current_timestamp as updated_date
from latest_employees s
left join public.employee e 
on cast(s.employee_number as int) = e.employee_number 
where e.employee_number is null

"""


class HRDataProcessor:

    def __init__(self):
        # self.drop_tables = drop_employee_tables()
        pass


    # to create employee related tables 

    def create_database_objects(self):
        conn = connect_to_postgres()
        cur = conn.cursor()
        cur.execute(employee_ddl)
        cur.execute(employee_hist_ddl)
        cur.execute(employee_stg_ddl)
        conn.commit()
        conn.close()


    def ingest_new_files(self, curr_date):
        #check for the files in incoming path in the given format
        # if file not found, raise value error
        # if available - dump the data as is into employee staging table as is and archive the file
        
        dir_path = '/Users/akhilavudatha/workspace/Auth0/incoming/'
        archive_dir_path = '/Users/akhilavudatha/workspace/Auth0/archive/'
        filename = curr_date+'.csv'
        print(filename)

        for root, dir, files in os.walk(dir_path):
            if filename in files:
                print('file found')
                conn = connect_to_postgres()
                cur = conn.cursor()
                with open(dir_path+filename, 'r') as f:
                    next(f)
                    #cur.execute('truncate public.employee_stg;')
                    try:
                        cur.copy_from(f, 'employee_stg', sep = ',', null='NULL')
                    except OSError:
                        print('could not read/open file', f)
                        raise
                    conn.commit()
                    conn.close()
                f.close()
                print('stage table loaded')

                os.replace(dir_path+filename, archive_dir_path+filename)
                print('file archived')
            else:
                raise ValueError('{} is not present in the incoming folder'.format(filename))

    # runs the sql scripts to load data into employee and employee_hist table

    def load_data_into_database(self,curr_date):
        conn = connect_to_postgres()
        cur = conn.cursor()
        cur.execute(insert_employee_hist.format(curr_date=curr_date))
        cur.execute(delete_employee.format(curr_date=curr_date))
        cur.execute(update_employee.format(curr_date=curr_date))
        cur.execute(insert_employee.format(curr_date=curr_date))
        conn.commit()
        conn.close()
        print('employee_hist and employee table loaded successfully!!')

    
    # to load historical data at once

    def load_historical_data(self):
        date_list = ['2020-01-01','2020-01-02','2020-01-03',
        '2020-01-04','2020-01-05','2020-01-06','2020-01-07',
        '2020-01-08','2020-01-09','2020-01-10']
        for each in date_list:
            self.create_database_objects()
            self.ingest_new_files(each)
            self.load_data_into_database(each)
            print("Processing for {} is done.".format(each))
        