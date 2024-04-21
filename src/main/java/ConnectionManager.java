import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionManager {

    private static final String PROTOCOL_POSTGRES_SQL = "jdbc:postgresql://";   // URL-prefix
    private static final String DRIVER_POSTGRES_SQL = "org.postgresql.Driver";  // Driver name

    private static final String URL_LOCALE_NAME = "localhost/";         // ваш компьютер
    private static final String URL_LOCALE_ADDRESS = "127.0.0.1:5432/"; // и это тоже ваш локальный компьютер
    private static final String URL_REMOTE = "10.242.65.114:5432/";     // FIXME IP-адрес кафедрального сервера

    private static final String DATABASE_NAME = "rut_library";          // FIXME имя базы

    public static final String DATABASE_URL = PROTOCOL_POSTGRES_SQL + URL_LOCALE_NAME + DATABASE_NAME;
    public static final String USER_NAME = "postgres";                  // FIXME имя пользователя
    public static final String DATABASE_PASS = "postgres";              // FIXME пароль базы данных

    public static void checkDriver () {
        try {
            Class.forName(DRIVER_POSTGRES_SQL);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB () {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }
}
