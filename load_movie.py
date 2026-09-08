import os
from datetime import datetime

import requests
import mysql.connector
from dotenv import load_dotenv


# ============================================================
# 1. LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY")

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")


# ============================================================
# 2. VALIDATE ENVIRONMENT VARIABLES
# ============================================================

required_variables = {
    "TMDB_API_KEY": TMDB_API_KEY,
    "MYSQL_HOST": MYSQL_HOST,
    "MYSQL_PORT": MYSQL_PORT,
    "MYSQL_DATABASE": MYSQL_DATABASE,
    "MYSQL_USER": MYSQL_USER,
    "MYSQL_PASSWORD": MYSQL_PASSWORD,
}

missing_variables = [
    name for name, value in required_variables.items()
    if not value
]

if missing_variables:
    raise RuntimeError(
        "Missing environment variables: "
        + ", ".join(missing_variables)
    )


# ============================================================
# 3. TMDB API
# ============================================================

url = "https://api.themoviedb.org/3/movie/popular"

params = {
    "api_key": TMDB_API_KEY,
    "language": "en-US",
    "page": 1
}

print("=" * 60)
print("MOVIE DATA PIPELINE")
print("=" * 60)

print("Calling TMDB...")

try:

    response = requests.get(
        url,
        params=params,
        timeout=30
    )

    print("TMDB Status Code:", response.status_code)

except requests.exceptions.RequestException as e:

    # Do NOT print e because it may contain the API key
    print("TMDB connection failed.")

    raise RuntimeError(
        "TMDB API connection failed."
    ) from e


# ============================================================
# 4. CHECK TMDB RESPONSE
# ============================================================

if response.status_code != 200:

    print("TMDB API request failed.")
    print("HTTP Status:", response.status_code)

    raise RuntimeError(
        f"TMDB API request failed with status "
        f"{response.status_code}"
    )


# ============================================================
# 5. EXTRACT MOVIE DATA
# ============================================================

data = response.json()

movies = data["results"]

print("Movies extracted:", len(movies))


# ============================================================
# 6. CONNECT TO MYSQL
# ============================================================

print("Connecting to MySQL...")

try:

    connection = mysql.connector.connect(
        host=MYSQL_HOST,
        port=int(MYSQL_PORT),
        database=MYSQL_DATABASE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )

    cursor = connection.cursor()

    print("MySQL connection successful")

except mysql.connector.Error as e:

    # Do not expose database credentials/details
    print("MySQL connection failed.")

    raise RuntimeError(
        "MySQL database connection failed."
    ) from e


# ============================================================
# 7. TRANSFORMATION + LOAD
# ============================================================

insert_query = """
INSERT INTO movies (
    movie_id,
    title,
    release_date,
    rating,
    popularity,
    language,
    adult,
    overview,
    rating_category,
    popularity_category,
    movie_age
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s
)

ON DUPLICATE KEY UPDATE

    title = VALUES(title),
    release_date = VALUES(release_date),
    rating = VALUES(rating),
    popularity = VALUES(popularity),
    language = VALUES(language),
    adult = VALUES(adult),
    overview = VALUES(overview),
    rating_category = VALUES(rating_category),
    popularity_category = VALUES(popularity_category),
    movie_age = VALUES(movie_age),
    loaded_at = CURRENT_TIMESTAMP
"""


loaded_count = 0


try:

    for movie in movies:

        # ----------------------------------------------------
        # Extract
        # ----------------------------------------------------

        movie_id = movie.get("id")

        title = movie.get("title")

        release_date = movie.get("release_date")

        rating = movie.get("vote_average")

        popularity = movie.get("popularity")

        language = movie.get("original_language")

        adult = movie.get("adult")

        overview = movie.get("overview")


        # ----------------------------------------------------
        # Handle missing release date
        # ----------------------------------------------------

        if release_date == "":
            release_date = None


        # ----------------------------------------------------
        # Rating Category
        # ----------------------------------------------------

        if rating is None:

            rating_category = "Unknown"

        elif rating >= 8:

            rating_category = "Excellent"

        elif rating >= 6:

            rating_category = "Good"

        else:

            rating_category = "Poor"


        # ----------------------------------------------------
        # Popularity Category
        # ----------------------------------------------------

        if popularity is None:

            popularity_category = "Unknown"

        elif popularity >= 100:

            popularity_category = "High"

        elif popularity >= 50:

            popularity_category = "Medium"

        else:

            popularity_category = "Low"


        # ----------------------------------------------------
        # Movie Age
        # ----------------------------------------------------

        if release_date:

            try:

                release_year = int(
                    release_date[:4]
                )

                current_year = datetime.now().year

                movie_age = current_year - release_year

            except (ValueError, TypeError):

                movie_age = None

        else:

            movie_age = None


        # ----------------------------------------------------
        # Load into MySQL
        # ----------------------------------------------------

        values = (
            movie_id,
            title,
            release_date,
            rating,
            popularity,
            language,
            adult,
            overview,
            rating_category,
            popularity_category,
            movie_age
        )

        cursor.execute(
            insert_query,
            values
        )

        loaded_count += 1


    # --------------------------------------------------------
    # Commit transaction
    # --------------------------------------------------------

    connection.commit()

    print("=" * 60)
    print("MOVIE LOAD COMPLETED")
    print("=" * 60)

    print("Movies loaded:", loaded_count)


except mysql.connector.Error as e:

    connection.rollback()

    print("MySQL data load failed.")

    raise RuntimeError(
        "MySQL data load failed."
    ) from e


finally:

    if "cursor" in locals():
        cursor.close()

    if "connection" in locals() and connection.is_connected():
        connection.close()

    print("MySQL connection closed.")


print("=" * 60)
print("PIPELINE COMPLETED SUCCESSFULLY")
print("=" * 60)