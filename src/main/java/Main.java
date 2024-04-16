import java.sql.*;

public class Main {
    public static void main(String[] args) {

        // проверка возможности подключения
        ConnectionManager.checkDriver();
        ConnectionManager.checkDB();

        // попытка открыть соединение с базой данных, которое java-закроет перед выходом из try-with-resources
        try (Connection connection = DriverManager.getConnection(ConnectionManager.DATABASE_URL, ConnectionManager.USER_NAME, ConnectionManager.DATABASE_PASS)) {

            // посмотрим на данные
            getAuthors (connection); System.out.println();
            getPublishers(connection); System.out.println();
            getBooks (connection); System.out.println();

            // запрос на данные на нескольких таблицах
            getBooksCountFromAuthors(connection); System.out.println();
            getPagesFromAuthors(connection); System.out.println();

            // запросы на данные с параметрами
            getBooksFromAuthor(connection, "Толкин"); System.out.println();
            getBooksPageFromAuthor(connection, "Пушкин"); System.out.println();
            getBooksFromPublisher(connection, "Махаон", 500);

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
            param2 = rs.getString(columnName2); // значение ячейки, можно получить по имени, по умолчанию возвращается строка
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);    // если точно уверены в типе данных ячейки, можно его сразу преобразовать
            System.out.println(param0 + " | " + param1 + " " + param2);
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
            System.out.println("label - " + rs.getMetaData().getColumnLabel(i) +
                            ", typeName - " + rs.getMetaData().getColumnTypeName(i) +
                            ", class - " + rs.getMetaData().getColumnClassName(i)
            );
        }
        System.out.println();

        while (rs.next()) {  // пока есть данные
            for (int i = 1; i <= count; i++) param += rs.getString(i) + " ";
            System.out.println(param);
            param = "";
        }
    }

    // endregion // Simple SELECT-requests


    // region // Simple SELECT-requests with JOIN

    static void getBooksCountFromAuthors (Connection connection) throws SQLException {
        Statement statement = connection.createStatement();             // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT author.surname, author.name, COUNT (DISTINCT book.title) " +
                "FROM author JOIN book ON author.id = book.id_author " +
                "GROUP BY (author.id) ORDER BY author.surname;");        // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные
            System.out.println(rs.getString(1) + " " + rs.getString(2) + " | " + rs.getInt(3));
        }
    }

    static void getPagesFromAuthors (Connection connection) throws SQLException {
        Statement statement = connection.createStatement();             // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT author.surname, author.name, SUM(book.page_count) " +
                "FROM author JOIN book ON author.id = book.id_author " +
                "GROUP BY author.id;");        // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные
            System.out.println(rs.getString(1) + " " + rs.getString(2) + " | " + rs.getInt(3));
        }
    }

    // endregion // Simple SELECT-requests with JOIN


    // region // SELECT-requests with params

    static void getBooksFromAuthor (Connection connection, String author) throws SQLException {
        if (author == null || author.isBlank()) return;     // проверка "на дурака"

        PreparedStatement statement = connection.prepareStatement("SELECT author.surname, author.name, book.title, publisher.label " +
                "FROM author " +
                "JOIN book ON author.id = book.id_author " +
                "JOIN publisher ON book.id_publisher = publisher.id " +
                "WHERE author.surname = ?;");       // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, author);             // добавление параметров в запрос с учетом их типа и порядка; индексация с 1
        ResultSet rs = statement.executeQuery();    // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getString(1) + " " + rs.getString(2) + " | " + rs.getString(3) + " | " + rs.getString(4));
        }
    }

    static void getBooksPageFromAuthor(Connection connection, String author) throws SQLException {
        if (author == null || author.isBlank()) return;     // проверка "на дурака"

        PreparedStatement statement = connection.prepareStatement("SELECT author.surname, book.title, AVG(book.page_count) " +
                "FROM author JOIN book ON author.id = book.id_author " +
                "WHERE author.surname = ? " +
                "GROUP BY (author.id, book.title);");       // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, author);             // добавление параметров в запрос с учетом их типа и порядка; индексация с 1
        ResultSet rs = statement.executeQuery();    // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getString(1) + " | " + rs.getString(2) + " | " + rs.getDouble(3));
        }
    }

    static void getBooksFromPublisher (Connection connection, String publisher, int pages) throws SQLException {
        if (publisher == null || publisher.isBlank() || pages <= 0) return;     // проверка "на дурака"

        PreparedStatement statement = connection.prepareStatement("SELECT book.title, book.page_count, author.name, author.surname " +
                "FROM author " +
                "JOIN book ON author.id = book.id_author " +
                "JOIN publisher ON book.id_publisher = publisher.id " +
                "WHERE publisher.label = ? AND book.page_count > ? " +
                "ORDER BY book.title");       // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, publisher);    // добавление параметров в запрос с учетом их типа и порядка
        statement.setInt(2, pages);           // добавление параметров в запрос с учетом их типа и порядка
        ResultSet rs = statement.executeQuery();    // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getString(1) + " | " + rs.getInt(2) + " стр. | " + rs.getString(3) + " " + rs.getString(4));
        }
    }

    // endregion // SELECT-requests with params
}
