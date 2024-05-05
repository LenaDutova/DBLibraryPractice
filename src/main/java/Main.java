import java.sql.*;

public class Main {
    public static void main(String[] args) {

        // проверка возможности подключения
        ConnectionManager.checkDriver();
        ConnectionManager.checkDB();

        System.out.println("Подключение к базе данных | " + ConnectionManager.DATABASE_URL + "\n");

        // попытка открыть соединение с базой данных, которое java-закроет перед выходом из try-with-resources
        try (Connection connection = DriverManager.getConnection(ConnectionManager.DATABASE_URL, ConnectionManager.USER_NAME, ConnectionManager.DATABASE_PASS)) {

            // TODO запрос на данные в отдельных таблицах
            getAuthors (connection); System.out.println();
            getPublishers(connection); System.out.println();
            getBooks (connection); System.out.println();

            // TODO запрос на данные на нескольких таблицах
            getAll(connection); System.out.println();

            // TODO запросы на данные с параметрами
            getAuthorBooks(connection, "Пушкин", false); System.out.println();
            getAuthorBooks(connection, "Пушкин", true); System.out.println();
            getBookTitles (connection, 6, 500);

            // TODO запрос на коррекцию
            addAuthor(connection, "Чехов", "Антон Петрович"); // добавление
            updateAuthor(connection, "Чехов", "Антон Павлович"); // изменение
            deleteAuthor(connection, "Чехов"); // удаление

        } catch (SQLException e) {
            // При открытии соединения, выполнении запросов могут возникать различные ошибки
            // Согласно стандарту SQL:2008 в ситуациях нарушения ограничений уникальности (в т.ч. дублирования данных) возникают ошибки соответствующие статусу (или дочерние ему): SQLState 23000 - Integrity Constraint Violation
            if (e.getSQLState().startsWith("23")){
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }
    }

    // region // Simple SELECT-requests

    static void getAuthors (Connection connection) throws SQLException {

        // имена столбцов
        String columnName0 = "id", columnName1 = "name", columnName2 = "surname";
        // значения ячеек
        int param0 = -1;
        String param1 = null, param2 = null;

        Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM author;"); // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные, продвигаться по ним
            param2 = rs.getString(columnName2); // значение ячейки, можно получить по имени; по умолчанию возвращается строка
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);    // если точно уверены в типе данных ячейки, можно его сразу преобразовать
            System.out.println(param0 + " | " + param1 + " | " + param2);
        }
    }

    static void getPublishers (Connection connection) throws SQLException {
        // значения ячеек
        int param0 = -1;
        String param1 = null;

        Statement statement = connection.createStatement();                 // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM publisher;");  // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные
            param0 = rs.getInt(1); // значение ячейки, можно также получить по порядковому номеру (начиная с 1)
            param1 = rs.getString(2);
            System.out.println(param0 + " | " + param1);
        }
    }

    static void getBooks (Connection connection) throws SQLException {
        String param = "";

        Statement statement = connection.createStatement();             // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM book;");   // выполняем запроса на поиск и получаем список ответов

        int count = rs.getMetaData().getColumnCount();  // сколько столбцов в ответе
        for (int i = 1; i <= count; i++){
            // что в этом столбце?
            System.out.println("position - " + i +
                            ", label - " + rs.getMetaData().getColumnLabel(i) +
                            ", type - " + rs.getMetaData().getColumnType(i) +
                            ", typeName - " + rs.getMetaData().getColumnTypeName(i) +
                            ", javaClass - " + rs.getMetaData().getColumnClassName(i)
            );
        }
        System.out.println();

        while (rs.next()) {  // пока есть данные
            for (int i = 1; i <= count; i++) {
                param += rs.getString(i);
                if (i != count) param += " | ";
            }
            System.out.println(param);
            param = "";
        }
    }

    // endregion // Simple SELECT-requests


    // region // Simple SELECT-requests with JOIN

    static void getAll (Connection connection) throws SQLException {
        Statement statement = connection.createStatement(); // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * " +
                "FROM author " +
                "JOIN book ON author.id = book.id_author " +
                "JOIN publisher ON book.id_publisher = publisher.id " +
                "ORDER BY author.surname;");                // выполняем запроса на поиск и получаем список ответов

        String param = "";
        int count = rs.getMetaData().getColumnCount();      // сколько столбцов в ответе
        while (rs.next()) {  // пока есть данные
            for (int i = 1; i <= count; i++) {
                param += rs.getString(i);
                if (i != count) param += " | ";
            }
            System.out.println(param);
            param = "";
        }
    }

    // endregion // Simple SELECT-requests with JOIN


    // region // SELECT-requests with params

    static void getAuthorBooks (Connection connection, String authorSurname, boolean fromSQL) throws SQLException {
        if (authorSurname == null || authorSurname.isBlank()) return;// проверка "на дурака"
        if (fromSQL) {
            getAuthorBooks(connection, authorSurname);               // если флаг верен, то выполняем аналогичный запрос c условием (WHERE)
        } else {
            long time = System.currentTimeMillis();
            Statement statement = connection.createStatement();      // создаем оператор для простого запроса (без параметров)
            ResultSet rs = statement.executeQuery(
                    "SELECT author.surname, author.name, book.title " +
                    "FROM author " +
                    "JOIN book ON author.id = book.id_author");      // выполняем запроса на поиск и получаем список ответов

            while (rs.next()) {  // пока есть данные перебираем их
                if (rs.getString(1).equals(authorSurname)) { // и выводим только определенный параметр
                    System.out.println(rs.getString(1) + " | " + rs.getString(2) + " | " + rs.getString(3));
                }
            }
            System.out.println("SELECT ALL and FIND (" + (System.currentTimeMillis() - time) + " мс.)");
        }
    }

    static void getAuthorBooks (Connection connection, String authorSurname) throws SQLException {
        if (authorSurname == null || authorSurname.isBlank()) return;     // проверка "на дурака"

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT author.surname, author.name, book.title " +
                "FROM author " +
                "JOIN book ON author.id = book.id_author " +
                "WHERE author.surname = ?;");       // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, authorSurname);      // "безопасное" добавление параметров в запрос; с учетом их типа и порядка (индексация с 1)
        ResultSet rs = statement.executeQuery();    // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getString(1) + " | " + rs.getString(2) + " | " + rs.getString(3));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    static void getBookTitles(Connection connection, int titleMinSize, int pageMinCount) throws SQLException{
        if (titleMinSize < 0 || pageMinCount < 0) return;     // проверка "на дурака"

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT id, title, page_count " +
                        "FROM book " +
                        "WHERE char_length(title) > ? AND page_count > ?;");       // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setInt(1, titleMinSize);      // "безопасное" добавление параметров в запрос; с учетом их типа и порядка (индексация с 1)
        statement.setInt(2, pageMinCount);      // "безопасное" добавление параметров в запрос; с учетом их типа и порядка (индексация с 1)
        ResultSet rs = statement.executeQuery();// выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getString(1) + " | " + rs.getString(2) + " | " + rs.getString(3));
        }
        System.out.println("SELECT with complex WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    // endregion // SELECT-requests with params


    // region // INSERT-request

    static void addAuthor (Connection connection, String surname, String name) throws SQLException{
        if (surname == null || surname.isBlank() || name == null || name.isBlank()) return;     // проверка "на дурака"

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO author(surname, name) VALUES (?, ?) returning id;", Statement.RETURN_GENERATED_KEYS);    // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, surname);    // "безопасное" добавление фамилии
        statement.setString(2, name);       // "безопасное" добавление имени

        int count =
                statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк
        System.out.println("Добавлено авторов " + count);

        ResultSet rs = statement.getGeneratedKeys(); // прочитать запрошенные данные от БД
        if (rs.next()) { // прокрутить к первой записи, если они есть
            System.out.println("Идентификатор автора " + rs.getInt(1));
        }
    }

    // endregion


    // region // UPDATE-request by given name

    static void updateAuthor (Connection connection, String keySurname, String newName) throws SQLException{
        if (keySurname == null || keySurname.isBlank() || newName == null || newName.isBlank()) return;     // проверка "на дурака"

        PreparedStatement statement = connection.prepareStatement("UPDATE author SET name=? WHERE surname=?;");
        statement.setString(1, newName);
        statement.setString(2, keySurname);

        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк
        System.out.println("Изменено авторов " + count);
    }

    // endregion


    // region // DELETE-request by given name

    static void deleteAuthor (Connection connection, String keySurname) throws SQLException{
        if (keySurname == null || keySurname.isBlank()) return;     // проверка "на дурака"

        PreparedStatement statement = connection.prepareStatement("DELETE from author WHERE surname=?;");
        statement.setString(1, keySurname);

        int count = statement.executeUpdate(); // выполняем запрос на удаление и возвращаем количество измененных строк
        System.out.println("Удалено авторов " + count);
    }

    // endregion
}
