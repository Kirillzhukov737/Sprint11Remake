package ru.yandex.practicum.filmorate.storage.genre;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.filmorate.constants.GenreTableConstants;
import ru.yandex.practicum.filmorate.customExceptions.DataNotFoundException;
import ru.yandex.practicum.filmorate.model.Genre;

import java.util.ArrayList;
import java.util.List;

@Component()
public class GenreDao implements GenreStorage {

    private final JdbcTemplate jdbcTemplate;

    public GenreDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public Genre getGenreById(int id) {
        String query = String.format(
                "SELECT * FROM %s WHERE %s = ?",
                GenreTableConstants.TABLE_NAME,
                GenreTableConstants.GENRE_ID
        );
        SqlRowSet genreRow = jdbcTemplate.queryForRowSet(query, id);
        Genre genre = new Genre();
        if (genreRow.next()) {
            genre = new Genre(id, genreRow.getString(GenreTableConstants.GENRE_NAME));
            return genre;
        }
        throw new DataNotFoundException("Жанр с индексом " + id + " не найден");
    }

    @Override
    public List<Genre> getAllGenres() {
        String query = String.format("SELECT * FROM %s", GenreTableConstants.TABLE_NAME);
        SqlRowSet genreRow = jdbcTemplate.queryForRowSet(query);
        List<Genre> genres = new ArrayList<>();
        while (genreRow.next()) {
            int id = genreRow.getInt(GenreTableConstants.GENRE_ID);
            String name = genreRow.getString(GenreTableConstants.GENRE_NAME);
            genres.add(new Genre(id, name));
        }
        return genres;
    }
}