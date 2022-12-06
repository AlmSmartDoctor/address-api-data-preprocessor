import psycopg2

class Database():
    def __init__(self):
        self.db = psycopg2.connect(host='localhost', dbname='auto_receipt', user='almighty', password='almighty', port=5432)
        self.cursor = self.db.cursor()

    def __del__(self):
        self.db.close()
        self.cursor.close()

    def execute(self, query, args={}):
        self.cursor.execute(query, args)
        row = self.cursor.fetchall()
        return row

    def commit(self):
        self.cursor.commit()

    def insert_db(self, table, colum, data):
        if str(type(data)) == "<class 'str'>":
            sql = f" INSERT INTO {table}({colum}) VALUES ('{data}') ;"
        elif len(data) == 2:
            sql = f" INSERT INTO {table}({colum[0]},{colum[1]}) VALUES ('{data[0]}','{data[1]}') ;"
        elif len(data) == 3:
            sql = f" INSERT INTO {table}({colum[0]},{colum[1]},{colum[2]}) VALUES ('{data[0]}','{data[1]}','{data[2]}') ;"
        else:
            print(f"데이터의 길이가 {len(data)}입니다.")

        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            print(" insert DB  ", e)