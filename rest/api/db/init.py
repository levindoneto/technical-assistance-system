import mysql.connector

def initDb():
    db_connection = mysql.connector.connect(
        host="hostname",
        user="username",
        passwd="password"
    )

    print(db_connection)

    db_cursor = db_connection.cursor()

    return db_cursor