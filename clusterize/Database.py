import psycopg2
import base64
import os
import credentials

READ_ERROR =  'READ_ERROR'

class DatabaseService:
    _instance = None

    def __init__(self):
        self.connection, self.cursor = self.open_connection()

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
          cls._instance = cls()
        return cls._instance

    def open_connection(self):
        try:
          connection = psycopg2.connect(user = credentials.USER,
                                        password = credentials.PASSWORD,
                                        host = credentials.HOST,
                                        port = credentials.PORT,
                                        database = credentials.DATABASE)

          cursor = connection.cursor()
          print ( connection.get_dsn_parameters(),"\n")

          cursor.execute("SELECT version();")
          record = cursor.fetchone()
          print("You are connected to - ", record,"\n")
          return connection, cursor

        except (Exception, psycopg2.Error) as error :
          print ("Error while connecting to PostgreSQL", error)

    
    def insert_cluster(self, id, cluster):
        query = "UPDATE crawler_results SET cluster_by_url = '{}' WHERE id = {}".format(cluster, id)
        self.cursor.execute(query)
        self.connection.commit()
    
    def insert_cluster_on_hyperlinks_pairs(self, id, cluster):
        query = "UPDATE hyperlink_pairs SET cluster_by_url = '{}' WHERE id = {}".format(cluster, id)
        self.cursor.execute(query)
        self.connection.commit()
    
    def get_id_and_url_from_specific_label(self, label):
        query = "SELECT id, url FROM crawler_results WHERE label = '{}' ORDER BY id ASC".format(label)
        self.cursor.execute(query)
        query_result_list = self.cursor.fetchall()
        return list(map(lambda x: (x[0], x[1]), query_result_list))
    
    def target_link_generator(self, label):
        start_id = None
        batch_size = 2000
        list_of_tuples = [1]
        while len(list_of_tuples):
            query = f'''select hp.id, hp.target_url from hyperlink_pairs hp 
               join crawler_results cr on cr.id = hp.crawler_results_id
               where cr.label = '{label}' and hp.id >= {start_id or 0} order by hp.id limit {batch_size}'''
            self.cursor.execute(query)
            query_result_list = self.cursor.fetchall()
            list_of_tuples = list(map(lambda x: (x[0], x[1]), query_result_list))
            try:
                start_id = list_of_tuples[-1][0] + 1
                for i in list_of_tuples:
                    yield i
            except:
                list_of_tuples = []


if __name__ == '__main__':
    db = DatabaseService.get_instance()
    a = db.get_analysis_info_from_crawler_results_id(1)
    print(a)
