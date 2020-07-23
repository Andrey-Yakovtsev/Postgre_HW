import psycopg2 as pg
import datetime

def delete_tables(cur):
    cur.execute('''
        DROP TABLE IF EXISTS Courses_Students;
        DROP TABLE IF EXISTS Student;
        DROP TABLE IF EXISTS Course;
        ''')

def create_tables(cur): # создает таблицы
    cur.execute('''
                CREATE TABLE IF NOT EXISTS
                Student(
                student_id SERIAL PRIMARY KEY,
                name VARCHAR(30),
                gpa NUMERIC(3, 2) NULL,
                birthdate TIMESTAMPTZ NULL);
                ''')

    cur.execute('''
                CREATE TABLE IF NOT EXISTS
                Course(
                course_id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL
                );
                ''')

    cur.execute('''
                CREATE TABLE IF NOT EXISTS
                Courses_Students(
                id INTEGER PRIMARY KEY,
                stud_id INTEGER REFERENCES Student(student_id),
                course_id INTEGER REFERENCES Course(course_id)
                );
                ''')



def add_student(cur, name, gpa, birthdate):
    cur.execute(
        '''INSERT INTO Student (name, gpa, birthdate) VALUES (%s, %s, %s);
        ''', (name, gpa, birthdate))

def add_courses(cur, name): # добавляет новый курс
    cur.execute(
        '''INSERT INTO Course (name) VALUES (%s);
        ''', (name,))

def get_courses_list(cur): # возвращает студента
    cur.execute('''
            SELECT * FROM Course;
            ''')
    for course in cur.fetchall():
        print('Our courses ==> ', course)

def add_students(course_id, name): # создает студентов и
                                       # записывает их на курс
    cur.execute(
       '''INSERT INTO Course (course_id, name) VALUES (%s, %s);
       ''', (course_id, name))

def get_course_students(cur, course_id):
    ''' (course_id): # возвращает студентов определенного курса'''
    cur.execute('''
        SELECT stud_id FROM Courses_Students WHERE course_id=%s;
        ''', (course_id,))
    for stud_id in cur.fetchone():
        print(f'На курс {course_id} ходит судент', stud_id)  #ТУТ КАКОЙ ТО БАГ В ВЫВОДЕ

def connect_studs_to_courses(cur, id, stud_id, course_id):
    cur.execute(
    '''INSERT INTO Courses_Students (id, stud_id, course_id) VALUES (%s, %s, %s);
            ''', (id, stud_id, course_id))
    cur.execute('''
                SELECT * FROM Courses_Students;
                ''')
    # for connections in cur.fetchall():
    #     print('Our connections ==> ', connections)

def get_student(cur, student_id): # возвращает студента
    cur.execute('''
            SELECT name, birthdate FROM Student WHERE student_id=%s;
            ''', (student_id,))
    for student in cur.fetchall():
        print('Our students ==> ', student)



with pg.connect(database='yakovtsevdb',
                user='yakovtsevdb',
                password='passw0rd',
                host='pg.codecontrol.ru',
                port=59432) as conn:

    cur = conn.cursor()

    delete_tables(cur)

    create_tables(cur)


    add_courses(cur, 'Advanced_Py_new')
    add_courses(cur, 'JS Master')
    add_student(cur, 'Firststudent', 5.78, '01.01.2020')
    add_student(cur, 'Secondstudent', 9.78, None)

    get_student(cur, 2)

    get_courses_list(cur)
    connect_studs_to_courses(cur, 1, 2, 1)
    connect_studs_to_courses(cur, 2, 1, 2)
    get_course_students(cur, 1)
    get_course_students(cur, 2)
