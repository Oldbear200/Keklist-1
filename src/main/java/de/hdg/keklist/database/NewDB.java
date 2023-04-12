package de.hdg.keklist.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.hdg.keklist.Keklist;
import org.bukkit.Bukkit;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.sql.*;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public class NewDB {

    private final DBType type;
    private final Keklist plugin;
    private static HikariConfig config = new HikariConfig();
    private static HikariDataSource ds;

    public NewDB(DBType dbType, Keklist plugin) {
        this.plugin = plugin;
        type = dbType;
    }

    public void connect() {
        try {
            switch (type) {
                case SQLITE -> {
                    File file = new File(Keklist.getInstance().getDataFolder(), "database.db");
                    if (!file.exists())
                        file.createNewFile();

                    String url = "jdbc:sqlite:" + file.getPath();
                    config.setJdbcUrl(url);
                }

                case MARIADB -> {
                    Class.forName("org.mariadb.jdbc.Driver");

                    String url = "jdbc:mariadb://";

                    String host = plugin.getConfig().getString("mariadb.host");
                    String port = plugin.getConfig().getString("mariadb.port");
                    String database = plugin.getConfig().getString("mariadb.database");
                    String username = plugin.getConfig().getString("mariadb.username");
                    String password = plugin.getConfig().getString("mariadb.password");
                    String options = plugin.getConfig().getString("mariadb.options");

                    url += host + ":" + port + "/" + database + options;

                    config.setJdbcUrl(url);
                    config.setUsername(username);
                    config.setPassword(password);
                }
            }

            ds = new HikariDataSource(config);
            createTables();
        } catch (java.io.IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException e) {
            plugin.getLogger().severe(Keklist.getLanguage().get("database.driver-missing"));
            Bukkit.getPluginManager().disablePlugin(plugin);
        }
    }


    @SuppressWarnings({"rawtypes", "unchecked"})
    public void onUpdate(@NotNull @Language("SQL") final String statement, Object... preparedArgs) {
        if (isConnected()) {
            new FutureTask(new Runnable() {
                PreparedStatement preparedStatement;

                public void run() {
                    try {
                        this.preparedStatement = connection.prepareStatement(statement);

                        for (int i = 0; i < preparedArgs.length; i++) {
                            this.preparedStatement.setObject(i + 1, preparedArgs[i]);
                        }

                        this.preparedStatement.executeUpdate();
                        this.preparedStatement.close();
                    } catch (SQLException throwable) {
                        throwable.printStackTrace();
                    }
                }
            }, null).run();
        } else {
            connect();
            onUpdate(statement, preparedArgs);
        }
    }

    public DBCon onQuery(@NotNull @Language("SQL") final String query, Object... preparedArgs) throws SQLException {
        Connection connection = getConnection();

        FutureTask<ResultSet> task = new FutureTask<>(new Callable<ResultSet>() {
            public ResultSet call() throws Exception {
                PreparedStatement ps = prepareStatement(connection, query, preparedArgs);

                for (int i = 0; i < preparedArgs.length; i++) {
                    ps.setObject(i + 1, preparedArgs[i]);
                }

                return ps.executeQuery();
            }
        });

        return new DBCon(connection, task);
    }

    public void a() throws ExecutionException, InterruptedException {

        doQuery("SELECT * FROM whitelist WHERE uuid = ?", rs -> {
            try (rs) {
                rs.next();
                return rs.getString(1);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, UUID.randomUUID()).get();
    }

    private Connection getConnection() throws SQLException {

        DBCon db = onQuery("SELECT 1");
        Future<ResultSet> rs = db.getResultSet();
        Connection con = db.getConnection();


        return ds.getConnection();
    }

    public <T> Future<T> doQuery(@Language("SQL") String sql, Function<ResultSet, T> processor, Object... params) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection con = getConnection();
                 PreparedStatement ps = prepareStatement(con, sql, params);
                 ResultSet rs = ps.executeQuery()) {
                return processor.apply(rs);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @NotNull
    private PreparedStatement prepareStatement(@NotNull Connection connection, @NotNull @Language("SQL") final String sql, @Nullable Object... preparedArgs) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(sql);

        for (int i = 0; i < preparedArgs.length; i++) {
            ps.setObject(i + 1, preparedArgs[i]);
        }

        return ps;
    }

    private void createTables() {
        onUpdate("CREATE TABLE IF NOT EXISTS whitelist (uuid VARCHAR(36) PRIMARY KEY, name VARCHAR(16) UNIQUE, byPlayer VARCHAR(16), unix BIGINT(13))");
        onUpdate("CREATE TABLE IF NOT EXISTS whitelistIp (ip VARCHAR(39) PRIMARY KEY, byPlayer VARCHAR(16), unix BIGINT(13))");

        onUpdate("CREATE TABLE IF NOT EXISTS blacklist (uuid VARCHAR(36) PRIMARY KEY, name VARCHAR(16) UNIQUE, byPlayer VARCHAR(16), unix BIGINT(13), reason VARCHAR(1500) DEFAULT 'No reason given')");
        onUpdate("CREATE TABLE IF NOT EXISTS blacklistIp (ip VARCHAR(39) PRIMARY KEY, byPlayer VARCHAR(16), unix BIGINT(13), reason VARCHAR(1500) DEFAULT 'No reason given')");
        onUpdate("CREATE TABLE IF NOT EXISTS blacklistMotd (ip VARCHAR(39) PRIMARY KEY, byPlayer VARCHAR(16), unix BIGINT(13))");
    }

    /**
     * Database types supported by the plugin
     */
    public enum DBType {
        MARIADB, SQLITE
    }

    private static class DBCon {
        private final Connection con;
        private final Future<ResultSet> rs;

        public DBCon(Connection con, FutureTask<ResultSet> rs) {
            this.con = con;
            this.rs = rs;

            rs.run();
        }

        public Connection getConnection() {
            return con;
        }

        public Future<ResultSet> getResultSet() {
            return rs;
        }
    }
}
