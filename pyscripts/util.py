import psycopg2

def connect_to_postgres():
    conn = psycopg2.connect(host="localhost", database="postgres", user="postgres", password="postgres")
    return conn

def drop_employee_tables():
    conn = connect_to_postgres()
    cur = conn.cursor()
    cur.execute('drop table public.employee_stg')
    cur.execute('drop table public.employee')
    cur.execute('drop table public.employee_hist')
    conn.commit()
    cur.close()
        

