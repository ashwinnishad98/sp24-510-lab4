import pandas as pd
import psycopg2


class Database:
    """
    A class representing a database connection.

    Attributes:
        con: The database connection object.
        cur: The database cursor object.

    Methods:
        __init__: Initializes the Database object.
        __enter__: Enters the context manager.
        __exit__: Exits the context manager.
        create_table: Creates the 'books' table in the database.
        insert_book: Inserts a book into the 'books' table.
        check_db_empty: Checks if the 'books' table is empty.
        query_books: Queries the 'books' table based on a search parameter.
    """

    def __init__(self, database_url) -> None:
        """
        Initializes the Database object.

        Args:
            database_url: The URL of the database.

        Returns:
            None
        """
        self.con = psycopg2.connect(database_url)
        self.cur = self.con.cursor()

    def __enter__(self):
        """
        Enters the context manager.

        Returns:
            self: The Database object.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exits the context manager.

        Args:
            exc_type: The type of the exception (if any).
            exc_val: The value of the exception (if any).
            exc_tb: The traceback of the exception (if any).

        Returns:
            None
        """
        self.con.close()

    def create_table(self):
        """
        Creates the 'books' table in the database.

        Returns:
            None
        """
        q = """
        CREATE TABLE IF NOT EXISTS books (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            price NUMERIC(10, 2) NOT NULL,
            rating TEXT NOT NULL,
            description TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        self.cur.execute(q)
        self.con.commit()

    def insert_book(self, book):
        """
        Inserts a book into the 'books' table.

        Args:
            book: A dictionary containing the book details.

        Returns:
            None
        """
        q = """
        INSERT INTO books (title, price, rating, description) VALUES (%s, %s, %s, %s)
        """
        self.cur.execute(
            q,
            (
                book["Title"],
                book["Price"],
                book["Rating"],
                book["Description"],
            ),
        )
        self.con.commit()

    def check_db_empty(self):
        """
        Checks if the 'books' table is empty.

        Returns:
            bool: True if the table is empty, False otherwise.
        """
        q = """
        SELECT EXISTS (SELECT 1 FROM books)
        """
        self.cur.execute(q)
        return self.cur.fetchone()[0]

    def query_books(self, param):
        """
        Queries the 'books' table based on a search parameter.

        Args:
            param: The search parameter.

        Returns:
            pd.DataFrame: A DataFrame containing the queried books.
        """
        q = """
        SELECT title, price, rating, description FROM books WHERE title LIKE %s OR description LIKE %s
        """
        params = (f"%{param}%", f"%{param}%")
        self.cur.execute(q, params)
        rows = self.cur.fetchall()
        columns = [desc[0] for desc in self.cur.description]
        df = pd.DataFrame(rows, columns=columns)
        return df
