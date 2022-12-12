package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;


import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {
    private final Connection connection = Util.getConnection();
    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {      //создание таблицы
        Connection connection = Util.getConnection();
        PreparedStatement preparedStatement = null;
        String sql = "CREATE TABLE IF NOT EXISTS users" + "(id BIGINT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(140), lastName VARCHAR(140), age int)";
        try(Statement statement =connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.execute(sql);
            connection.commit();
            System.out.println("Таблица создана");
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public void dropUsersTable() {       //удаление таблицы
        
        String sql = "DROP TABLE if EXISTS users";

        try(Statement statement =connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.execute(sql);
            connection.commit();
            System.out.println("Таблица удалена");
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }


    public void saveUser(String name, String lastName, byte age) {    //добавление юзера
        String sql = "INSERT INTO users" + "(name, lastName, age)" + " VALUES (?, ?, ?)";
        try (PreparedStatement preparedStatement =connection.prepareStatement(sql)) {
            connection.setAutoCommit(false);
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setLong(3,age);
            preparedStatement.executeUpdate();
            connection.commit();
            System.out.println("User с именем - " + name + " добавлен в базу данных");
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }


    public void removeUserById(long id) {    //удаление по айди

        String sql = "DELETE FROM users WHERE ID = ?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            connection.setAutoCommit(false);
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();
            connection.commit();
            System.out.println("Пользователь с ID " + id + " был удален");
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                connection.rollback();


            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }



    public List<User> getAllUsers() {

        List<User> users = new ArrayList <> ();
        String sql = "SELECT * FROM users;";
        try(Statement statement =connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql)) {
            connection.setAutoCommit(false);
            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong("id"));
                user.setName(resultSet.getString("name"));
                user.setLastName(resultSet.getString("lastName"));
                user.setAge(resultSet.getByte("age"));
                users.add(user);
            }
            connection.commit();
            System.out.println("Таблица пользователей выведена: ");
            System.out.println(users);
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
        return users;
    }

    public void cleanUsersTable() {   //очищение таблицы

        String sql = "TRUNCATE TABLE users";
        try(Statement statement =connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.execute(sql);
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                connection.rollback();
                System.out.println("Таблица очищена");
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}