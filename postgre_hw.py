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
                id SERIAL PRIMARY KEY,
                stud_id INTEGER REFERENCES Student(student_id),
                course_id INTEGER REFERENCES Course(course_id)
                );
                ''')



def add_student(cur, students_list):
    '''Берет из списка студентов запись и создает ее в БД. Табличка Student '''
    for student in students_list:
        cur.execute(
            '''INSERT INTO Student (name, gpa, birthdate) VALUES (%s, %s, %s)
            RETURNING student_id;
            ''', (student['name'], student['gpa'], student['birthdate']))

def add_courses(cur, course_list): # добавляет курсы из списка в базу
    for course in course_list:
        cur.execute(
        '''INSERT INTO Course (name) VALUES (%s);
        ''', (course['name'],))

def get_courses_list(cur): # возвращает список курсов
    cur.execute('''
            SELECT * FROM Course;
            ''')
    for course in cur.fetchall():
        print('Our courses ==> ', course)

def add_students(cur, students_list, course_id):
    ''' # создает студентов и # записывает их на курс'''
    for student in students_list:
        cur.execute(
            '''INSERT INTO Student (name, gpa, birthdate) VALUES (%s, %s, %s)
            RETURNING student_id;
            ''', (student['name'], student['gpa'], student['birthdate']))
        student_id = cur.fetchall()[0][0]

        cur.execute(
            '''INSERT INTO Courses_Students (stud_id, course_id) VALUES (%s, %s);
            ''', (student_id, course_id))


def get_course_students(cur, course_id):
    ''' (course_id): # возвращает студентов определенного курса'''
    cur.execute('''
        SELECT stud_id FROM Courses_Students WHERE course_id=%s;
        ''', (course_id,))
    for stud_id in cur.fetchone():
        print(f'На курс {course_id} ходит судент', stud_id)


def get_student(cur, student_id): # возвращает студента
    cur.execute('''
            SELECT student_id, name, birthdate FROM Student WHERE student_id=%s;
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

    adpy_students_list = [
        {'name': 'FirstStud', 'gpa': 4.56, 'birthdate': '01-01-1999'},
        {'name': 'SecondStud', 'gpa': 6.33, 'birthdate': None},
    ]
    js_students_list = [
        {'name': '3RD_Stud', 'gpa': 9.99, 'birthdate': '01-12-2000'},
        {'name': 'Fourth_Stud', 'gpa': 4.44, 'birthdate': '04-12-1444'},
    ]
    courses_list = [
        {'name': 'AdvancedPY'},
        {'name': 'JS_Pro'},
        {'name': 'DjangoMaster'}
    ]


    add_courses(cur, courses_list)
    add_students(cur, adpy_students_list, 1)
    add_students(cur, js_students_list, 2)

    get_student(cur, 2)
    get_student(cur, 1)
    get_student(cur, 3)
    print()
    get_courses_list(cur)
    # print()

    get_course_students(cur, 1)
    get_course_students(cur, 2)
