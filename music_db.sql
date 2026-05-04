DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS song_genres;
DROP TABLE IF EXISTS songs;
DROP TABLE IF EXISTS albums;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS genres;
DROP TABLE IF EXISTS artists;

CREATE TABLE artists (
    artist_id INT AUTO_INCREMENT PRIMARY KEY,
    artist_name VARCHAR(255) NOT NULL UNIQUE,
    artist_type ENUM('individual', 'group') NOT NULL DEFAULT 'individual'
);

CREATE TABLE genres (
    genre_id INT AUTO_INCREMENT PRIMARY KEY,
    genre_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE albums (
    album_id INT AUTO_INCREMENT PRIMARY KEY,
    album_name VARCHAR(255) NOT NULL,
    artist_id INT NOT NULL,
    release_date DATE NOT NULL,
    genre_id INT NOT NULL,
    UNIQUE (album_name, artist_id),
    FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
        ON DELETE RESTRICT ON UPDATE CASCADE
);

CREATE TABLE songs (
    song_id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    artist_id INT NOT NULL,
    album_id INT NULL,
    release_date DATE NOT NULL,
    UNIQUE (title, artist_id),
    FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (album_id) REFERENCES albums(album_id)
        ON DELETE SET NULL ON UPDATE CASCADE
);

CREATE TABLE song_genres (
    song_id INT NOT NULL,
    genre_id INT NOT NULL,
    PRIMARY KEY (song_id, genre_id),
    FOREIGN KEY (song_id) REFERENCES songs(song_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
        ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE users (
    user_id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE ratings (
    rating_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    song_id INT NOT NULL,
    rating TINYINT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    rating_date DATE NOT NULL,
    UNIQUE (user_id, song_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (song_id) REFERENCES songs(song_id)
        ON DELETE CASCADE ON UPDATE CASCADE
);
