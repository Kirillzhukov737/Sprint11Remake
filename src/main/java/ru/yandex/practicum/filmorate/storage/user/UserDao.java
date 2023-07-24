package ru.yandex.practicum.filmorate.storage.user;

import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.filmorate.constants.FriendShipStatusConstants;
import ru.yandex.practicum.filmorate.constants.FriendsListConstants;
import ru.yandex.practicum.filmorate.constants.UserTableConstants;
import ru.yandex.practicum.filmorate.customExceptions.InstanceAlreadyExistException;
import ru.yandex.practicum.filmorate.customExceptions.DataNotFoundException;
import ru.yandex.practicum.filmorate.customExceptions.ValidationException;
import ru.yandex.practicum.filmorate.model.User;

import java.sql.Date;
import java.time.LocalDate;
import java.util.*;

@Component()
@Slf4j
public class UserDao implements UserDbStorage {

    private final JdbcTemplate jdbcTemplate;

    public UserDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Возвращает таблицу всех пользователей
     *
     * @return - HashMap<Integer,User>
     */
    @Override
    public HashMap<Integer, User> getAllUsers() {
        HashMap<Integer, User> users = new HashMap<>();
        String query = String.format(
                "SELECT %s AS user_id FROM %s",
                UserTableConstants.USER_ID,
                UserTableConstants.TABLE_NAME
        );
        SqlRowSet userRows = jdbcTemplate.queryForRowSet(query);
        while (userRows.next()) {
            int index = userRows.getInt("user_id");
            users.put(index, loadUserFromDbById(index));
        }
        log.info("Переданы все пользователи");
        return users;
    }

    /**
     * Добавляет пользователя в таблицу
     *
     * @param user User добавляемый пользователь
     * @return - User в случае успешного добавления пользователя возвращает добавленный объект
     */
    @Override
    public User addUser(User user) {
        checkUserValidation(user);
        try {
            if (isPresentInDataBase(user)) {
                throw new InstanceAlreadyExistException("Не удалось добавить пользователя: пользователь уже существует");
            }
            jdbcTemplate.update(
                    String.format("INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)",
                            UserTableConstants.TABLE_NAME,
                            UserTableConstants.EMAIL,
                            UserTableConstants.LOGIN,
                            UserTableConstants.NAME,
                            UserTableConstants.BIRTHDAY),
                    user.getEmail(),
                    user.getLogin(),
                    user.getName(),
                    Date.valueOf(user.getBirthday())
            );

            user = loadUserFromDbById(findUserId(user));
            log.info("Добавлен пользователь {}", user);
            return user;
        } catch (DataIntegrityViolationException | BadSqlGrammarException ex) {
            SqlRowSet rowSet = getUserIdRow(user);
            if (rowSet.next()) {
                deleteUser(rowSet.getInt(UserTableConstants.USER_ID));
            }
            throw new RuntimeException("SQL exception");
        }
    }

    private int findUserId(User user) {
        SqlRowSet usersRows = getUserIdRow(user);
        if (usersRows.next()) {
            return usersRows.getInt(UserTableConstants.USER_ID);
        }
        throw new DataNotFoundException("user not found");
    }

    private SqlRowSet getUserIdRow(User user) {
        String query = String.format(
                "SELECT %s\nFROM %s\nWHERE %s=? AND %s=?",
                UserTableConstants.USER_ID,
                UserTableConstants.TABLE_NAME,
                UserTableConstants.LOGIN,
                UserTableConstants.EMAIL
        );
        SqlRowSet usersRows = jdbcTemplate.queryForRowSet(query, user.getLogin(), user.getEmail());
        return usersRows;
    }

    /**
     * Обновляет пользователя в таблице
     *
     * @param user обновленная версия пользователя, содержит идентификатор Id
     * @return - User в случае успешного обновления пользователя возвращает добавленный объект
     */
    @Override
    public User updateUser(User user) {
        checkUserValidation(user);
        User buffUser = new User();
        try {
            if (isPresentInDataBase(user.getId())) {
                buffUser = loadUserFromDbById(user.getId());
                String query = String.format(
                        "UPDATE %s SET %s=?, %s=?, %s=?, %s=? WHERE %s=?;",
                        UserTableConstants.TABLE_NAME,
                        UserTableConstants.EMAIL,
                        UserTableConstants.LOGIN,
                        UserTableConstants.NAME,
                        UserTableConstants.BIRTHDAY,
                        UserTableConstants.USER_ID
                );
                jdbcTemplate.update(
                        query,
                        user.getEmail(),
                        user.getLogin(),
                        user.getName(),
                        Date.valueOf(user.getBirthday()),
                        user.getId()
                );
                user = loadUserFromDbById(user.getId());
                log.info("Обновлен пользователь {}", user);
                return user;
            }
            throw new DataNotFoundException("Не удалось обновить пользователя: пользователь не найден.");
        } catch (DataIntegrityViolationException | BadSqlGrammarException ex) {
            updateUser(buffUser);
            throw new RuntimeException("SQL exception");
        }
    }

    /**
     * Удаляет пользователя с идентификатором id из таблицы
     *
     * @param id идентификатор пользователя, которого необходимо удалить
     * @return - User копия удаленного пользователя возвращается в случае успешного удаления из таблицы
     */
    @Override
    public User deleteUser(int id) {
        if (!isPresentInDataBase(id)) {
            throw new DataNotFoundException("Не удалось удалить пользователя: пользователь не найден.");
        }
        User removingUser = loadUserFromDbById(id);
        String deleteQuery = String.format(
                "DELETE FROM %s WHERE %s = ?",
                UserTableConstants.TABLE_NAME,
                UserTableConstants.USER_ID
        );
        jdbcTemplate.update(deleteQuery, id);
        log.info("Удален пользователь {}", removingUser);
        if (getAllUsers().isEmpty()) {
            jdbcTemplate.execute(
                    String.format(
                            "ALTER TABLE %s ALTER COLUMN %s RESTART WITH 1",
                            UserTableConstants.TABLE_NAME,
                            UserTableConstants.USER_ID
                    )
            );
        }
        return removingUser;
    }

    /**
     * Удаляет всех пользователей из таблицы, восстанавливает счетчик идентификаторов
     */
    @Override
    public void deleteAllUsers() {
        SqlRowSet idsRows = jdbcTemplate.queryForRowSet("SELECT * FROM " + UserTableConstants.TABLE_NAME);
        while (idsRows.next()) {
            int userId = idsRows.getInt(UserTableConstants.USER_ID);
            String deleteQuery = String.format(
                    "DELETE FROM %s WHERE %s = ?",
                    UserTableConstants.TABLE_NAME,
                    UserTableConstants.USER_ID
            );
            jdbcTemplate.update(deleteQuery, userId);
        }
        String restartQuery = String.format(
                "ALTER TABLE %s ALTER COLUMN %s RESTART WITH 1",
                UserTableConstants.TABLE_NAME,
                UserTableConstants.USER_ID
        );
        jdbcTemplate.execute(restartQuery);
        log.info("Таблица пользователей очищена");
    }

    @Override
    public void makeFriends(int userId, int friendId) {
        if (!isPresentInDataBase(userId) || !isPresentInDataBase(friendId)) {
            throw new DataNotFoundException("Не удалось удалить пользователя: пользователь не найден.");
        }
        if (areTheyFriends(userId, friendId)) {
            throw new InstanceAlreadyExistException("Они уже друзья");
        }
        insertIntoFriendList(userId, friendId, User.FriendStatus.NOT_ACCEPTED);
        updateFriendShipStatus(userId, friendId);
        log.info("Пользователь {} и {} записаны в базу друзей", userId, friendId);
    }

    private void updateFriendShipStatus(int userId, int friendId) {
        String subQuery = String.format(
                "SELECT %s AS id FROM %s WHERE %s IN (%d, %d) AND %s IN (%d, %d)",
                FriendsListConstants.USER_ID,
                FriendsListConstants.TABLE_NAME,
                FriendsListConstants.USER_ID,
                userId,
                friendId,
                FriendsListConstants.FRIEND_ID,
                userId,
                friendId
        );

        String query = String.format(
                "SELECT COUNT(ft.id) AS count FROM (%s) AS ft",
                subQuery
        );

        SqlRowSet friendsRows = jdbcTemplate.queryForRowSet(query);
        if (friendsRows.next()) {
            if (friendsRows.getInt("count") == 2) {
                String updateQuery = String.format(
                        "UPDATE %s SET %s = ? WHERE %s IN (%d, %d) AND %s IN (%d, %d)",
                        FriendsListConstants.TABLE_NAME,
                        FriendsListConstants.FRIENDSHIP_STATUS_ID,
                        FriendsListConstants.USER_ID,
                        userId,
                        friendId,
                        FriendsListConstants.FRIEND_ID,
                        userId,
                        friendId
                );
                jdbcTemplate.update(updateQuery, User.FriendStatus.ACCEPTED.ordinal() + 1);
            }
        }
    }

    @Override
    public void deleteFriends(int userId, int friendId) {
        if (!areTheyFriends(userId, friendId)) {
            throw new DataNotFoundException("Пользователи " + userId + " и " + friendId + " не дружили");
        }
        deleteFromFriendList(userId, friendId);
    }

    private void deleteFromFriendList(int userId, int friendId) {
        String deleteQuery = String.format(
                "DELETE FROM %s WHERE %s IN (%d, %d) AND %s IN (%d, %d);",
                FriendsListConstants.TABLE_NAME,
                FriendsListConstants.USER_ID,
                userId,
                friendId,
                FriendsListConstants.FRIEND_ID,
                userId,
                friendId
        );
        jdbcTemplate.execute(deleteQuery);
    }

    private void insertIntoFriendList(int userId, int friendId, User.FriendStatus status) {
        jdbcTemplate.update(
                "INSERT INTO " + FriendsListConstants.TABLE_NAME
                        + " (" + FriendsListConstants.USER_ID
                        + "," + FriendsListConstants.FRIEND_ID
                        + "," + FriendsListConstants.FRIENDSHIP_STATUS_ID + ")\n"
                        + "VALUES (?,?,?);",
                userId, friendId, status.ordinal() + 1);
    }

    private boolean areTheyFriends(int userId, int friendId) {
        SqlRowSet friendsRows = jdbcTemplate.queryForRowSet(
                "SELECT " + FriendsListConstants.USER_ID
                        + "\nFROM " + FriendsListConstants.TABLE_NAME
                        + "\nWHERE " + FriendsListConstants.USER_ID + " IN (?) AND "
                        + FriendsListConstants.FRIEND_ID + " IN (?);",
                userId, friendId);
        if (friendsRows.next()) {
            return true;
        }
        return false;
    }

    /**
     * Возвращает пользователя по идентификатору
     *
     * @param id идентификатор пользователя, которого необходимо передать
     * @return User пользователь с запрошенным id
     * @throws Exception - пользователь с указанным id не найден в таблице
     */
    @Override
    public User getUserById(int id) {
        User user = loadUserFromDbById(id);
        log.info("Передан пользователь id = {}", id);
        return user;
    }

    /**
     * Создает объект User по данным из БД по первичному ключу
     *
     * @param id первичный ключ таблицы
     * @return объект User
     */
    private User loadUserFromDbById(int id) {
        String userQuery = String.format(
                "SELECT * FROM %s WHERE %s = ?",
                UserTableConstants.TABLE_NAME,
                UserTableConstants.USER_ID
        );
        SqlRowSet userRows = jdbcTemplate.queryForRowSet(userQuery, id);

        User user = User.builder().build();
        if (userRows.next()) {
            user.setId(id);
            user.setLogin(userRows.getString(UserTableConstants.LOGIN));
            user.setEmail(userRows.getString(UserTableConstants.EMAIL));
            user.setName(userRows.getString(UserTableConstants.NAME));
            user.setBirthday(Date.valueOf(userRows.getString(UserTableConstants.BIRTHDAY)).toLocalDate());
            checkUserValidation(user);
        } else {
            throw new DataNotFoundException("Пользователь с id " + id + " не найден.");
        }

        String friendsQuery = String.format(
                "SELECT fl.%s, fs.%s\n"
                        + "FROM %s AS fl\n"
                        + "INNER JOIN %s AS fs ON fs.%s = fl.%s\n"
                        + "WHERE %s = ?",
                FriendsListConstants.FRIEND_ID,
                FriendShipStatusConstants.FRIENDSHIP_STATUS_NAME,
                FriendsListConstants.TABLE_NAME,
                FriendShipStatusConstants.TABLE_NAME,
                FriendShipStatusConstants.FRIENDSHIP_STATUS_ID,
                FriendsListConstants.FRIENDSHIP_STATUS_ID,
                FriendsListConstants.USER_ID
        );
        SqlRowSet friendsRows = jdbcTemplate.queryForRowSet(friendsQuery, id);

        while (friendsRows.next()) {
            int friendID = friendsRows.getInt(FriendsListConstants.FRIEND_ID);
            User.FriendStatus status = User.FriendStatus.valueOf(
                    friendsRows.getString(FriendShipStatusConstants.FRIENDSHIP_STATUS_NAME)
            );
            user.getFriendIdList().add(friendID);
            user.getFriendStatuses().put(friendID, status);
        }
        return user;
    }

    public boolean isPresentInDataBase(User user) {
        SqlRowSet usersRows = jdbcTemplate.queryForRowSet(
                "SELECT " + UserTableConstants.USER_ID + "\n"
                        + "FROM " + UserTableConstants.TABLE_NAME + "\n"
                        + "WHERE " + UserTableConstants.LOGIN + "=? OR " + UserTableConstants.EMAIL + "=?;",
                user.getLogin(), user.getEmail());
        if (usersRows.next()) {
            return true;
        }
        return false;
    }

    public boolean isPresentInDataBase(int userId) {
        SqlRowSet usersRows = jdbcTemplate.queryForRowSet(
                "SELECT " + UserTableConstants.USER_ID + "\n"
                        + "FROM " + UserTableConstants.TABLE_NAME + "\n"
                        + "WHERE " + UserTableConstants.USER_ID + " =?;",
                userId);
        if (usersRows.next()) {
            return true;
        }
        return false;
    }

    /**
     * Проверяет поля пользователя на корректность
     *
     * @param user пользователь, чьи поля необходимо проверить
     */
    private void checkUserValidation(User user) {
        StringBuilder message = new StringBuilder().append("Не удалось добавить пользователя: ");
        boolean isValid = true;
        if (user.getLogin().contains(" ")) {
            message.append("неверный формат логина; ");
            isValid = false;
        }
        if (user.getName() == null || user.getName().isBlank()) {
            user.setName(user.getLogin());
        }
        if (user.getBirthday().isAfter(LocalDate.now())) {
            message.append("пользователь из будущего; ");
            isValid = false;
        }
        if (!isValid) {
            throw new ValidationException(message.toString());
        }
    }
}