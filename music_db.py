from typing import List, Tuple, Set

def _execute(cursor, query, params=None):
    cursor.execute(query, params or ())

def _get_or_create_artist(cursor, artist_name: str, artist_type: str = "individual") -> int:
    _execute(cursor, "SELECT artist_id FROM artists WHERE artist_name = %s", (artist_name,))
    row = cursor.fetchone()
    if row:
        return row[0]

    _execute(
        cursor,
        "INSERT INTO artists (artist_name, artist_type) VALUES (%s, %s)",
        (artist_name, artist_type),
    )
    return cursor.lastrowid

def _get_or_create_genre(cursor, genre_name: str) -> int:
    _execute(cursor, "SELECT genre_id FROM genres WHERE genre_name = %s", (genre_name,))
    row = cursor.fetchone()
    if row:
        return row[0]

    _execute(cursor, "INSERT INTO genres (genre_name) VALUES (%s)", (genre_name,))
    return cursor.lastrowid

def _get_song(cursor, artist_name: str, song_title: str):
    _execute(
        cursor,
        """
        SELECT s.song_id
        FROM songs s
        JOIN artists a ON s.artist_id = a.artist_id
        WHERE a.artist_name = %s AND s.title = %s
        """,
        (artist_name, song_title),
    )
    row = cursor.fetchone()
    return row[0] if row else None

def clear_database(mydb):
    cursor = mydb.cursor()
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

    tables = [
        "ratings",
        "song_genres",
        "songs",
        "albums",
        "users",
        "genres",
        "artists",
    ]

    for table in tables:
        cursor.execute(f"DELETE FROM {table}")

    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    mydb.commit()
    cursor.close()

def load_single_songs(
    mydb,
    single_songs: List[Tuple[str, Tuple[str, ...], str, str]],
) -> Set[Tuple[str, str]]:
    inserted = set()
    cursor = mydb.cursor()

    for song_title, genres, artist_name, release_date in single_songs:
        if not genres:
            continue

        existing_song_id = _get_song(cursor, artist_name, song_title)
        if existing_song_id is not None:
            continue

        artist_id = _get_or_create_artist(cursor, artist_name, "individual")

        _execute(
            cursor,
            """
            INSERT INTO songs (title, artist_id, album_id, release_date)
            VALUES (%s, %s, NULL, %s)
            """,
            (song_title, artist_id, release_date),
        )
        song_id = cursor.lastrowid

        for genre_name in genres:
            genre_id = _get_or_create_genre(cursor, genre_name)
            _execute(
                cursor,
                "INSERT IGNORE INTO song_genres (song_id, genre_id) VALUES (%s, %s)",
                (song_id, genre_id),
            )

        inserted.add((artist_name, song_title))

    mydb.commit()
    cursor.close()
    return inserted

def load_albums(
    mydb,
    albums: List[Tuple[str, str, str, List[str]]],
) -> Set[Tuple[str, str]]:
    inserted = set()
    cursor = mydb.cursor()

    for album_name, artist_name, release_date, song_titles in albums:
        if not song_titles:
            continue

        artist_id = _get_or_create_artist(cursor, artist_name, "group")
        genre_id = _get_or_create_genre(cursor, "Unknown")

        _execute(
            cursor,
            """
            SELECT album_id
            FROM albums
            WHERE album_name = %s AND artist_id = %s
            """,
            (album_name, artist_id),
        )
        if cursor.fetchone():
            continue

        _execute(
            cursor,
            """
            INSERT INTO albums (album_name, artist_id, release_date, genre_id)
            VALUES (%s, %s, %s, %s)
            """,
            (album_name, artist_id, release_date, genre_id),
        )
        album_id = cursor.lastrowid

        for song_title in song_titles:
            if _get_song(cursor, artist_name, song_title) is not None:
                continue

            _execute(
                cursor,
                """
                INSERT INTO songs (title, artist_id, album_id, release_date)
                VALUES (%s, %s, %s, %s)
                """,
                (song_title, artist_id, album_id, release_date),
            )
            song_id = cursor.lastrowid

            _execute(
                cursor,
                "INSERT IGNORE INTO song_genres (song_id, genre_id) VALUES (%s, %s)",
                (song_id, genre_id),
            )

        inserted.add((artist_name, album_name))

    mydb.commit()
    cursor.close()
    return inserted

def load_users(mydb, users: List[str]) -> Set[str]:
    inserted = set()
    cursor = mydb.cursor()

    for username in users:
        _execute(cursor, "SELECT user_id FROM users WHERE username = %s", (username,))
        if cursor.fetchone():
            continue

        _execute(cursor, "INSERT INTO users (username) VALUES (%s)", (username,))
        inserted.add(username)

    mydb.commit()
    cursor.close()
    return inserted

def load_song_ratings(
    mydb,
    song_ratings: List[Tuple[str, Tuple[str, str], int, str]],
) -> Set[Tuple[str, str, str]]:
    inserted = set()
    cursor = mydb.cursor()

    for username, song_info, rating, rating_date in song_ratings:
        artist_name, song_title = song_info

        if rating not in {1, 2, 3, 4, 5}:
            continue

        _execute(cursor, "SELECT user_id FROM users WHERE username = %s", (username,))
        user_row = cursor.fetchone()
        if not user_row:
            continue
        user_id = user_row[0]

        song_id = _get_song(cursor, artist_name, song_title)
        if song_id is None:
            continue

        _execute(
            cursor,
            """
            SELECT rating_id
            FROM ratings
            WHERE user_id = %s AND song_id = %s
            """,
            (user_id, song_id),
        )
        if cursor.fetchone():
            continue

        _execute(
            cursor,
            """
            INSERT INTO ratings (user_id, song_id, rating, rating_date)
            VALUES (%s, %s, %s, %s)
            """,
            (user_id, song_id, rating, rating_date),
        )
        inserted.add((username, artist_name, song_title))

    mydb.commit()
    cursor.close()
    return inserted

def get_most_prolific_individual_artists(
    mydb,
    n: int,
    year_range: Tuple[int, int],
) -> List[Tuple[str, int]]:
    start_year, end_year = year_range
    cursor = mydb.cursor()
    _execute(
        cursor,
        """
        SELECT a.artist_name, COUNT(s.song_id) AS song_count
        FROM artists a
        JOIN songs s ON a.artist_id = s.artist_id
        WHERE a.artist_type = 'individual'
          AND YEAR(s.release_date) BETWEEN %s AND %s
        GROUP BY a.artist_id, a.artist_name
        ORDER BY song_count DESC, a.artist_name ASC
        LIMIT %s
        """,
        (start_year, end_year, n),
    )
    result = cursor.fetchall()
    cursor.close()
    return result

def get_artists_last_single_in_year(mydb, year: int) -> Set[str]:
    cursor = mydb.cursor()
    _execute(
        cursor,
        """
        SELECT a.artist_name
        FROM artists a
        JOIN songs s ON a.artist_id = s.artist_id
        WHERE s.album_id IS NULL
        GROUP BY a.artist_id, a.artist_name
        HAVING YEAR(MAX(s.release_date)) = %s
        """,
        (year,),
    )
    result = {row[0] for row in cursor.fetchall()}
    cursor.close()
    return result

def get_top_song_genres(mydb, n: int) -> List[Tuple[str, int]]:
    cursor = mydb.cursor()
    _execute(
        cursor,
        """
        SELECT g.genre_name, COUNT(sg.song_id) AS song_count
        FROM genres g
        JOIN song_genres sg ON g.genre_id = sg.genre_id
        GROUP BY g.genre_id, g.genre_name
        ORDER BY song_count DESC, g.genre_name ASC
        LIMIT %s
        """,
        (n,),
    )
    result = cursor.fetchall()
    cursor.close()
    return result

def get_album_and_single_artists(mydb) -> Set[str]:
    cursor = mydb.cursor()
    _execute(
        cursor,
        """
        SELECT a.artist_name
        FROM artists a
        JOIN songs s ON a.artist_id = s.artist_id
        GROUP BY a.artist_id, a.artist_name
        HAVING SUM(CASE WHEN s.album_id IS NULL THEN 1 ELSE 0 END) > 0
           AND SUM(CASE WHEN s.album_id IS NOT NULL THEN 1 ELSE 0 END) > 0
        """,
    )
    result = {row[0] for row in cursor.fetchall()}
    cursor.close()
    return result

def get_most_rated_songs(
    mydb,
    year_range: Tuple[int, int],
    n: int,
) -> List[Tuple[str, str, int]]:
    start_year, end_year = year_range
    cursor = mydb.cursor()
    _execute(
        cursor,
        """
        SELECT a.artist_name, s.title, COUNT(r.rating_id) AS rating_count
        FROM ratings r
        JOIN songs s ON r.song_id = s.song_id
        JOIN artists a ON s.artist_id = a.artist_id
        WHERE YEAR(r.rating_date) BETWEEN %s AND %s
        GROUP BY s.song_id, a.artist_name, s.title
        ORDER BY rating_count DESC, a.artist_name ASC, s.title ASC
        LIMIT %s
        """,
        (start_year, end_year, n),
    )
    result = cursor.fetchall()
    cursor.close()
    return result

def get_most_engaged_users(
    mydb,
    year_range: Tuple[int, int],
    n: int,
) -> List[Tuple[str, int]]:
    start_year, end_year = year_range
    cursor = mydb.cursor()
    _execute(
        cursor,
        """
        SELECT u.username, COUNT(r.rating_id) AS rating_count
        FROM users u
        JOIN ratings r ON u.user_id = r.user_id
        WHERE YEAR(r.rating_date) BETWEEN %s AND %s
        GROUP BY u.user_id, u.username
        ORDER BY rating_count DESC, u.username ASC
        LIMIT %s
        """,
        (start_year, end_year, n),
    )
    result = cursor.fetchall()
    cursor.close()
    return result