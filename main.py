import psycopg2

from psycopg2.extensions import connection

USER_NAME = ""
USER_PASSWORD = ""

def create_db(conn: connection) -> bool:
    cur = conn.cursor()

    try:
        cur.execute(""" CREATE TABLE IF NOT EXISTS client (
                            client_id SERIAL PRIMARY KEY,
                            first_name VARCHAR(80) NOT NULL,
                            second_name VARCHAR(80) NOT NULL,
                            email VARCHAR(320) NOT NULL UNIQUE);""")

        cur.execute(""" CREATE TABLE IF NOT EXISTS phone (
                            number_id SERIAL PRIMARY KEY,
                            client_id INTEGER NOT NULL REFERENCES client(client_id),
                            number VARCHAR(15) UNIQUE);""")

        conn.commit()

    except psycopg2.Error as ex:
        print(ex)
        conn.rollback()
        return False

    return True

def drop_db(conn: connection) -> bool:
    cur = conn.cursor()

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
    cur = conn.cursor()

    try:
        cur.execute(""" INSERT INTO client (first_name, second_name, email)
                              VALUES
                                (%s, %s, %s)
                              RETURNING client_id;""", [first_name, second_name, email])

        client_id = cur.fetchone()[0]

        for phone in phones:
            cur.execute(""" INSERT INTO phone (client_id, number)
                                  VALUES
                                    (%s, %s);""", [client_id, phone])

        conn.commit()

    except psycopg2.Error as ex:
        print(ex)
        conn.rollback()
        return False

    return True

def add_phone(conn: connection, client_id: int, phone: str):
    cur = conn.cursor()

    try:
        cur.execute(""" INSERT INTO phone (client_id, number)
                              VALUES
                                (%s, %s);""", [client_id, phone])

        conn.commit()

    except psycopg2.Error as ex:
        print(ex)
        conn.rollback()
        return False

    return True

def get_client_data(conn: connection, client_id: int):
    cur = conn.cursor()

    try:
        cur.execute(""" SELECT * FROM client
                              WHERE client_id = %s;""", [client_id])

        record = cur.fetchone()
        if record is None:
            print("Client wasn't found!")

    except psycopg2.Error as ex:
        print(ex)
        conn.rollback()
        return False

    return record

def get_client_phone_data(conn: connection, client_id: int):
    cur = conn.cursor()

    try:
        cur.execute(""" SELECT * FROM phone
                              WHERE client_id = %s;""", [client_id])

        records = cur.fetchall()
        if records is None:
            phone_list = []
        else:
            phone_list = [record[-1] for record in records]

    except psycopg2.Error as ex:
        print(ex)
        conn.rollback()
        return False

    return phone_list


def change_client_data(conn: connection, client_id: int,
                       first_name: str = None, second_name: str = None,
                       email: str = None, phones: list[str] = None):
    cur = conn.cursor()

    try:
        cur.execute(""" SELECT first_name, second_name, email
                              FROM client
                              WHERE client_id = %s""", [client_id])

        update_first_name, update_second_name, update_email = cur.fetchone()

        if first_name is not None:
            update_first_name = first_name
        if second_name is not None:
            update_second_name = second_name
        if email is not None:
            update_email = email

        cur.execute(""" UPDATE client
                              SET first_name = %s, second_name = %s, email = %s
                              WHERE client_id = %s;""", [update_first_name, update_second_name, update_email, client_id])

        if phones is not None:
            cur.execute(""" DELETE FROM phone
                                  WHERE client_id = %s;""", [client_id])

            for phone in phones:
                cur.execute(""" INSERT INTO phone (client_id, number)
                                      VALUES
                                        (%s, %s);""", [client_id, phone])

        conn.commit()

    except psycopg2.Error as ex:
        print(ex)
        conn.rollback()
        return False

    return True

def delete_phone(conn: connection, client_id: int, phone: str):
    cur = conn.cursor()

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
    cur = conn.cursor()

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
                email: str = None, phone: str = None):
    cur = conn.cursor()

    try:
        condition_list = []
        condition_value_list = []
        if first_name is not None:
            condition_list.append("first_name = %s")
            condition_value_list.append(first_name)
        if second_name is not None:
            condition_list.append("second_name = %s")
            condition_value_list.append(second_name)
        if email is not None:
            condition_list.append("email = %s")
            condition_value_list.append(email)

        if condition_value_list:
            cur.execute(f""" SELECT client_id FROM client
                                   WHERE {' AND '.join(condition_list)}; """, condition_value_list)
            found_record = cur.fetchone()
        else:
            found_record = None

        if found_record is None and phone:
            cur.execute(f"""SELECT client_id FROM phone
                                  WHERE number = %s;""", [phone])

            found_record = cur.fetchone()

        conn.commit()

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
        client_id_elon_found_by_number = find_client(conn, phone = "5555551234")

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

        drop_db(conn)