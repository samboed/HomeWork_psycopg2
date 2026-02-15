import psycopg2

from psycopg2.extensions import connection

USER_NAME = "postgres"
USER_PASSWORD = "secret123"

def create_db(conn: connection) -> bool:
    with conn.cursor() as cur:
        try:
            cur.execute(""" CREATE TABLE IF NOT EXISTS client (
                                client_id SERIAL PRIMARY KEY,
                                name VARCHAR(80) NOT NULL,
                                surname VARCHAR(80) NOT NULL,
                                email VARCHAR(320) NOT NULL UNIQUE,
                                CONSTRAINT proper_email CHECK (email ~* '^[A-Za-z0-9._+%-]+@[A-Za-z0-9.-]+[.][A-Za-z]+$')
                                );""")

            cur.execute(""" CREATE TABLE IF NOT EXISTS phone (
                                number_id SERIAL PRIMARY KEY,
                                number VARCHAR(15) NOT NULL UNIQUE,
                                client_id INTEGER NOT NULL REFERENCES client(client_id));""")

            conn.commit()

        except psycopg2.Error as ex:
            print(ex)
            conn.rollback()
            return False

        return True

def drop_db(conn: connection) -> bool:
    with conn.cursor() as cur:
        try:
            cur.execute("""DROP TABLE IF EXISTS client CASCADE;""")

            cur.execute("""DROP TABLE IF EXISTS phone CASCADE;""")

            conn.commit()

        except psycopg2.Error as ex:
            print(ex)
            conn.rollback()
            return False

        return True

def add_client(conn: connection, first_name: str, second_name: str, email: str, phones: list[str]) -> bool:
    with conn.cursor() as cur:
        try:
            cur.execute(""" INSERT INTO client (name, surname, email)
                                  VALUES
                                    (%s, %s, %s)
                                  RETURNING client_id;""", [first_name, second_name, email])

            client_id = cur.fetchone()[0]

            for phone in phones:
                cur.execute(""" INSERT INTO phone (number, client_id)
                                      VALUES
                                        (%s, %s);""", [phone, client_id])

            conn.commit()

        except psycopg2.Error as ex:
            print(ex)
            conn.rollback()
            return False

        return True

def add_phone(conn: connection, client_id: int, phone: str):
    with conn.cursor() as cur:
        try:
            cur.execute(""" INSERT INTO phone (number, client_id)
                                  VALUES
                                    (%s, %s);""", [phone, client_id])

            conn.commit()

        except psycopg2.Error as ex:
            print(ex)
            conn.rollback()
            return False

        return True

def get_client_data(conn: connection, client_id: int):
    with conn.cursor() as cur:
        try:
            cur.execute(""" SELECT * FROM client
                                  WHERE client_id = %s;""", [client_id])

            record = cur.fetchone()
            if record is None:
                print("The client was not found!")

        except psycopg2.Error as ex:
            print(ex)
            conn.rollback()
            return False

        return record

def get_client_phone_data(conn: connection, client_id: int):
    with conn.cursor() as cur:
        try:
            cur.execute(""" SELECT * FROM phone
                                  WHERE client_id = %s;""", [client_id])

            records = cur.fetchall()
            if records is None:
                phone_list = []
            else:
                phone_list = [record[1] for record in records]

        except psycopg2.Error as ex:
            print(ex)
            conn.rollback()
            return False

        return phone_list


def change_client_data(conn: connection, client_id: int,
                       first_name: str = None, second_name: str = None,
                       email: str = None, phones: list[str] = None):
    with conn.cursor() as cur:
        try:
            cur.execute(""" SELECT name, surname, email
                                  FROM client
                                  WHERE client_id = %s""", [client_id])

            update_name, update_surname, update_email = cur.fetchone()

            if first_name is not None:
                update_name = first_name
            if second_name is not None:
                update_surname = second_name
            if email is not None:
                update_email = email

            cur.execute(""" UPDATE client
                                  SET name = %s, surname = %s, email = %s
                                  WHERE client_id = %s;""", [update_name, update_surname, update_email, client_id])

            if phones is not None:
                cur.execute(""" DELETE FROM phone
                                      WHERE client_id = %s;""", [client_id])

                for phone in phones:
                    cur.execute(""" INSERT INTO phone (number, client_id)
                                          VALUES
                                            (%s, %s);""", [phone, client_id])

            conn.commit()

        except psycopg2.Error as ex:
            print(ex)
            conn.rollback()
            return False

        return True

def delete_phone(conn: connection, client_id: int, phone: str):
    with conn.cursor() as cur:
        try:
            if get_client_data(conn, client_id) is None:
                return False

            cur.execute(""" DELETE FROM phone
                                  WHERE client_id = %s AND number = %s;""", [client_id, phone])

            conn.commit()

        except psycopg2.Error as ex:
            print(ex)
            conn.rollback()
            return False

        return True

def delete_client(conn: connection, client_id: int):
    with conn.cursor() as cur:
        try:
            if get_client_data(conn, client_id) is None:
                return False

            cur.execute(""" DELETE FROM client
                                  WHERE client_id = %s;""", [client_id])

            cur.execute(""" DELETE FROM phone
                                  WHERE client_id = %s;""", [client_id])

            conn.commit()

        except psycopg2.Error as ex:
            print(ex)
            conn.rollback()
            return False

        return True

def find_client(conn: connection, first_name: str = None, second_name: str = None,
                email: str = None, phone_number: str = None):
    with conn.cursor() as cur:
        try:
            cur.execute(""" SELECT c.client_id FROM client AS c
                                  LEFT JOIN phone AS p
                                  ON c.client_id = p.client_id
                                  WHERE ((name = %(name)s) OR (%(name)s IS NULL))
                                  AND ((surname = %(surname)s) OR (%(surname)s IS NULL))
                                  AND ((email = %(email)s) OR (%(email)s IS NULL))
                                  AND ((number = %(number)s) OR (%(number)s IS NULL));""",
                        {"name": first_name, "surname": second_name, "email": email, "number": phone_number})

            found_record = cur.fetchone()

        except psycopg2.Error as ex:
            print(ex)
            conn.rollback()
            return False

        if found_record:
            return found_record[0]
        else:
            return None

if __name__ == "__main__":
    with psycopg2.connect(database="clients_db" ,user=USER_NAME, password=USER_PASSWORD) as conn:
        cur = conn.cursor()

        drop_db(conn)

        # CREATING TABLES
        create_db(conn)

        # ADDING
        add_client(conn, "Pavel", "Lomazov", "pavel.lomazov@mail.ru", [])
        add_client(conn, "Stephen", "Hawking", "stephen.hawking@gmail.com", ["2135550123", "2135554567"])
        add_client(conn, "Elon", "Mask", "elon.mask@gmail.com", ["5555551234"])

        # FINDING
        client_id_pavel = find_client(conn, first_name = "Pavel")
        client_id_stephen = find_client(conn, second_name = "Hawking")
        client_id_elon = find_client(conn, email = "elon.mask@gmail.com")
        client_id_elon_found_by_number = find_client(conn, phone_number="5555551234")

        assert client_id_pavel == 1
        assert client_id_stephen == 2
        assert client_id_elon == 3
        assert client_id_elon_found_by_number == 3

        client_data_pavel = get_client_data(conn, client_id_pavel)
        client_phone_data_pavel = get_client_phone_data(conn, client_id_pavel)

        assert client_data_pavel == tuple([1, 'Pavel', 'Lomazov', 'pavel.lomazov@mail.ru'])
        assert client_phone_data_pavel == []

        add_phone(conn, client_id_pavel, "89338779256")
        new_client_phone_data_pavel = get_client_phone_data(conn, client_id_pavel)
        assert new_client_phone_data_pavel == ["89338779256"] # adding phone

        delete_phone(conn, client_id_pavel, "89338779256")
        delete_client_phone_data_pavel = get_client_phone_data(conn, client_id_pavel)
        assert delete_client_phone_data_pavel == [] # deletion client phone

        res_delete_client = delete_client(conn, 99)
        assert res_delete_client is False # deletion unknown client

        res_delete_client = delete_client(conn, client_id_pavel)
        assert res_delete_client is True # deletion client

        res_find_unknown_client = find_client(conn, first_name="Pavel")
        assert res_find_unknown_client is None # find client which was deleted

        change_client_data(conn, client_id_stephen, first_name="Anonymous", second_name="Hawkinggg", email="stephen.hawkinggg@gmail.com",
                           phones=["123456789", "987654321"])

        new_client_data_stephen = get_client_data(conn, client_id_stephen)
        new_client_phone_data_stephen = get_client_phone_data(conn, client_id_stephen)

        assert new_client_data_stephen == tuple([client_id_stephen, 'Anonymous', 'Hawkinggg', 'stephen.hawkinggg@gmail.com'])
        assert new_client_phone_data_stephen == ["123456789", "987654321"]
