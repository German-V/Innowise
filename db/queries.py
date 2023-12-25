import files.reader

"""
A class for executing sql queries
"""

class Queries: 
    def __init__(self, cur,logg):
        self.cur = cur
        self.logg = logg
    
    def create(self, table_name: str, values: dict):
        try:
            exec = "CREATE TABLE IF NOT EXISTS "+table_name+"( "
            for key,value in values.items():
                exec+=f"{key} {value}"+","
            
            exec = exec.removesuffix(",")
            exec+=")"
            self.cur.execute(exec)
            self.logg.info("Database "+table_name+" created")
        except:
            self.logg.exception("Database "+table_name+"not created")
            
    def run_queries(self,file,out):
        self.q1(file,out)
        self.q2(file,out)
        self.q3(file,out)
        self.q4(file,out)
            
    def insert_data(self, table_name: str, insert_data: str):
        try:
            exec="INSERT INTO " + table_name + " VALUES\n "+insert_data
            self.cur.execute(exec)
            self.logg.info("values inserted into "+table_name)       
        except:
            self.logg.exception("Impossible to insert values into "+table_name)
    
    def q1(self, file: files.reader.FileWorker, out: str):
        self.cur.execute("SELECT r.name, count(s.id) FROM rooms r "+ 
        "JOIN students s ON r.id = s.room "+
        "GROUP BY r.name "+
        "ORDER BY r.name")
        file.write(out, self.logg, self.cur.fetchall(), ['name', 'count'])
        
    def q2(self, file: files.reader.FileWorker, out: str):
        self.cur.execute("SELECT r.name ,AVG(date_part('year', age(current_date, birthday))) avg_age FROM rooms r " +
        "JOIN students s ON r.id = s.room " +
        "GROUP BY r.name " +
        "ORDER BY avg_age " +
        "LIMIT 5")
        file.write(out, self.logg, self.cur.fetchall(), ['name', 'average_age'])
        
    def q3(self, file: files.reader.FileWorker, out: str):
        self.cur.execute("SELECT r.name, (MAX(date_part('year', age(current_date, birthday)))-MIN(date_part('year', age(current_date, birthday)))) diff FROM rooms r " +
        "JOIN students s ON r.id = s.room " +
        "GROUP BY r.id " + 
        "ORDER BY diff DESC " +
        "LIMIT 5")
        file.write(out, self.logg, self.cur.fetchall(), ['name', 'difference'])
    
    def q4(self, file: files.reader.FileWorker, out: str):
        self.cur.execute("SELECT room FROM students " +
        "WHERE sex = 'M' " +
        "GROUP BY room " +
        "INTERSECT " +
        "SELECT room FROM students " +
        "WHERE sex = 'F' " +
        "GROUP BY room " +
        "ORDER BY room")
        file.write(out, self.logg, self.cur.fetchall(), ['room'])