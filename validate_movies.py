import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

print("=" * 60)
print("MOVIE DATA QUALITY VALIDATION")
print("=" * 60)

# --------------------------------------------------
# Connect to MySQL
# --------------------------------------------------

try:
    connection = mysql.connector.connect(
        host=MYSQL_HOST,
        port=int(MYSQL_PORT),
        database=MYSQL_DATABASE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )

    cursor = connection.cursor(dictionary=True)

    print("MySQL connection successful")

except mysql.connector.Error as e:
    raise RuntimeError("MySQL connection failed.") from e


try:

    # --------------------------------------------------
    # 1. Check table is not empty
    # --------------------------------------------------

    cursor.execute("SELECT COUNT(*) AS total FROM movies")
    total_movies = cursor.fetchone()["total"]

    print(f"Total movies: {total_movies}")

    if total_movies == 0:
        raise RuntimeError("Data quality failed: movies table is empty.")

    # --------------------------------------------------
    # 2. Check required fields
    # --------------------------------------------------

    cursor.execute("""
        SELECT COUNT(*) AS invalid_rows
        FROM movies
        WHERE movie_id IS NULL
           OR title IS NULL
           OR title = ''
    """)

    invalid_rows = cursor.fetchone()["invalid_rows"]

    print(f"Invalid required-field rows: {invalid_rows}")

    if invalid_rows > 0:
        raise RuntimeError(
            f"Data quality failed: {invalid_rows} rows have missing required fields."
        )

    # --------------------------------------------------
    # 3. Check duplicate movie IDs
    # --------------------------------------------------

    cursor.execute("""
        SELECT COUNT(*) AS duplicate_groups
        FROM (
            SELECT movie_id
            FROM movies
            GROUP BY movie_id
            HAVING COUNT(*) > 1
        ) AS duplicates
    """)

    duplicate_groups = cursor.fetchone()["duplicate_groups"]

    print(f"Duplicate movie ID groups: {duplicate_groups}")

    if duplicate_groups > 0:
        raise RuntimeError(
            "Data quality failed: duplicate movie IDs found."
        )

    # --------------------------------------------------
    # 4. Check rating range
    # --------------------------------------------------

    cursor.execute("""
        SELECT COUNT(*) AS invalid_ratings
        FROM movies
        WHERE rating IS NOT NULL
          AND (rating < 0 OR rating > 10)
    """)

    invalid_ratings = cursor.fetchone()["invalid_ratings"]

    print(f"Invalid ratings: {invalid_ratings}")

    if invalid_ratings > 0:
        raise RuntimeError(
            "Data quality failed: ratings outside 0-10 range."
        )

    # --------------------------------------------------
    # 5. Check popularity
    # --------------------------------------------------

    cursor.execute("""
        SELECT COUNT(*) AS invalid_popularity
        FROM movies
        WHERE popularity IS NOT NULL
          AND popularity < 0
    """)

    invalid_popularity = cursor.fetchone()["invalid_popularity"]

    print(f"Invalid popularity values: {invalid_popularity}")

    if invalid_popularity > 0:
        raise RuntimeError(
            "Data quality failed: negative popularity values found."
        )

    # --------------------------------------------------
    # Validation successful
    # --------------------------------------------------

    print("=" * 60)
    print("DATA QUALITY VALIDATION PASSED")
    print("=" * 60)

finally:

    cursor.close()
    connection.close()

    print("MySQL connection closed.")