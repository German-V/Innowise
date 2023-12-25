"""
Main script for processing data from json files, inserting it into database, 
running queries and writing into xml or json

Usage:
python3 main.py -ro <rooms_path> -st <students_path> -out <output_format>

Example:
python3 main.py -ro sources/rooms.json -st sources/students.json -out json

Options:
  -ro, -rooms_path TEXT     insert path to rooms.json
  -st, -students_path TEXT  insert path to students.json
  -out TEXT                 insert output format - 'json' or 'xml'
  --help                    Show this message and exit.

"""



import db.queries as queries
import files.reader as reader
import db.connection as connection
import logging
import click


@click.command()
@click.option('-ro', '-rooms_path', help='insert path to rooms.json')
@click.option('-st', '-students_path', help='insert path to students.json')
@click.option('-out', help='insert output format - \'json\' or \'xml\'')
def main(ro, st, out):
    logging.basicConfig(level=logging.INFO, filename="py_log.log",
                        filemode="w",
                        format="%(asctime)s %(levelname)s %(message)s")
    logging.info("Program started")

    file = reader.FileWorker()
    Connection = connection.Connection(logging)
    conn = Connection.create_connection()

    rooms = file.read(ro, logging)
    
    students = file.read(st, logging)

    cur = conn.cursor()

    Q = queries.Queries(cur, logging)

    Q.create('rooms', {'id': 'INT PRIMARY KEY', 'name': 'VARCHAR(10)'})
    Q.create('students', {'id': 'INT PRIMARY KEY',
            'birthday': 'DATE',
            'name': 'VARCHAR(50)',
            'room': 'INT',
            'sex': 'VARCHAR(1)'})
    conn.commit()
    Q.insert_data("rooms", insert_rooms(rooms, logging))
    Q.insert_data("students", insert_students(students, logging))
    Q.run_queries(file, out)
    Connection.close_connection(conn)
    logging.info("Program finished")





def insert_rooms(rooms: list, logging: logging) -> str:
    insert_rooms = ''
    room_id, room_name = rooms_data(rooms, logging)
    for i in range(0,len(room_id)):
        insert_rooms += f"({room_id[i]}, '{room_name[i]}'),\n"
    return insert_rooms.removesuffix(',\n')

def insert_students(students: list, logging: logging) -> str:
    insert_students = ''
    student_id, student_birthday, student_name, student_room, student_sex = student_data(students, logging)
    for i in range(0,len(student_id)):
        insert_students += f"({student_id[i]}, '{student_birthday[i]}', '{student_name[i]}', {student_room[i]}, '{student_sex[i]}'),\n"
    return insert_students.removesuffix(',\n')  

def rooms_data(rooms: list, logging: logging):
    try:
        return [item['id'] for item in rooms], [item['name'] for item in rooms]
    except: 
        logging.exception("File is empty")

def student_data(students: list, logging: logging):
    try:
        return [item['id'] for item in students], \
            [item['birthday'] for item in students], \
            [item['name'] for item in students], \
            [item['room'] for item in students], \
            [item['sex'] for item in students]
    except: 
        logging.exception("File is empty")


if __name__ == '__main__':
    main()

