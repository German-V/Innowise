import unittest
from unittest.mock import MagicMock
import db.connection as connection
import logging
import main
import db.queries as queries

class TestConnection(unittest.TestCase):
    def setUp(self):
        
        self.logger = logging
        self.connection = connection.Connection(self.logger)            

    # def test_create_connection(self):
    #     conn = self.connection.create_connection()

    #     self.assertIsNotNone(conn)
    #     print(conn)
    #     self.connection.close_connection(conn)  
        



    def test_close_connection(self):
        mock_conn = self.connection.create_connection()

        self.connection.close_connection(mock_conn)
        self.assertEqual(mock_conn.closed, 1)


class TestMain(unittest.TestCase):
        
    def test_insert_rooms(self):
            rooms = [{'id': 1, 'name': 'Room1'}, {'id': 2, 'name': 'Room2'}]
            logging_mock = MagicMock()
            result = main.insert_rooms(rooms, logging_mock)

            expected_result = "(1, 'Room1'),\n(2, 'Room2')"
            self.assertEqual(result, expected_result)

    def test_insert_students(self):
            students = [
                {'id': 1, 'birthday': '2000-01-01', 'name': 'Student1', 'room': 1, 'sex': 'Male'},
                {'id': 2, 'birthday': '1999-02-02', 'name': 'Student2', 'room': 2, 'sex': 'Female'}
            ]
            logging_mock = MagicMock()

            result = main.insert_students(students, logging_mock)

            expected_result = (
                "(1, '2000-01-01', 'Student1', 1, 'Male'),\n"
                "(2, '1999-02-02', 'Student2', 2, 'Female')"
            )
            self.assertEqual(result, expected_result)

    def test_rooms_data(self):
            rooms = [{'id': 1, 'name': 'Room1'}, {'id': 2, 'name': 'Room2'}]
            logging_mock = MagicMock()

            result = main.rooms_data(rooms, logging_mock)

            expected_result = ([1, 2], ['Room1', 'Room2'])
            self.assertEqual(result, expected_result)

    def test_student_data(self):
            students = [
                {'id': 1, 'birthday': '2000-01-01', 'name': 'Student1', 'room': 1, 'sex': 'Male'},
                {'id': 2, 'birthday': '1999-02-02', 'name': 'Student2', 'room': 2, 'sex': 'Female'}
            ]
            logging_mock = MagicMock()

            result = main.student_data(students, logging_mock)

            expected_result = ([1, 2], ['2000-01-01', '1999-02-02'], ['Student1', 'Student2'], [1, 2], ['Male', 'Female'])
            self.assertEqual(result, expected_result)
    
class TestQueries(unittest.TestCase):
    
    def setUp(self):
        self.cursor_mock = MagicMock()
        self.logger_mock = MagicMock()
        self.queries = queries.Queries(self.cursor_mock, self.logger_mock)

    def test_create(self):
        table_name = "test_table"
        values = {"id": "INT", "name": "VARCHAR(255)"}
        self.queries.create(table_name, values)

        expected_sql = "CREATE TABLE IF NOT EXISTS test_table( id INT,name VARCHAR(255))"
        self.cursor_mock.execute.assert_called_once_with(expected_sql)

    def test_insert_data(self):
        table_name = "test_table"
        insert_data = "(1, 'John'), (2, 'Jane')"
        self.queries.insert_data(table_name, insert_data)

        expected_sql = "INSERT INTO test_table VALUES\n (1, 'John'), (2, 'Jane')"
        self.cursor_mock.execute.assert_called_once_with(expected_sql)
        
    def test_q1(self):
        file_mock = MagicMock()
        out = "output.txt"
        self.queries.q1(file_mock, out)

        expected_sql = "SELECT r.name, count(s.id) FROM rooms r JOIN students s ON r.id = s.room GROUP BY r.name ORDER BY r.name"
        self.cursor_mock.execute.assert_called_once_with(expected_sql)

        file_mock.write.assert_called_once_with(out, self.logger_mock, self.cursor_mock.fetchall(), ['name', 'count'])

    def test_q2(self):
        file_mock = MagicMock()
        out = "output.txt"
        self.queries.q2(file_mock, out)

        expected_sql = "SELECT r.name ,AVG(date_part('year', age(current_date, birthday))) avg_age FROM rooms r JOIN students s ON r.id = s.room GROUP BY r.name ORDER BY avg_age LIMIT 5"
        self.cursor_mock.execute.assert_called_once_with(expected_sql)

        file_mock.write.assert_called_once_with(out, self.logger_mock, self.cursor_mock.fetchall(), ['name', 'average_age'])

    def test_q3(self):
        file_mock = MagicMock()
        out = "output.txt"
        self.queries.q3(file_mock, out)

        expected_sql = "SELECT r.name, (MAX(date_part('year', age(current_date, birthday)))-MIN(date_part('year', age(current_date, birthday)))) diff FROM rooms r JOIN students s ON r.id = s.room GROUP BY r.id ORDER BY diff DESC LIMIT 5"
        self.cursor_mock.execute.assert_called_once_with(expected_sql)

        file_mock.write.assert_called_once_with(out, self.logger_mock, self.cursor_mock.fetchall(), ['name', 'difference'])

    def test_q4(self):
        file_mock = MagicMock()
        out = "output.txt"
        self.queries.q4(file_mock, out)

        expected_sql = "SELECT room FROM students WHERE sex = 'M' GROUP BY room INTERSECT SELECT room FROM students WHERE sex = 'F' GROUP BY room ORDER BY room"
        self.cursor_mock.execute.assert_called_once_with(expected_sql)

        file_mock.write.assert_called_once_with(out, self.logger_mock, self.cursor_mock.fetchall(), ['room'])



if __name__ == "__main__":
    unittest.main()
