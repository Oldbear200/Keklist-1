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
import java.util.concurrent.*;

public class NewDB {

    private final DBType type;
    private final Keklist plugin;
    private static final HikariConfig config = new HikariConfig();
    private HikariDataSource databaseSource;

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

                    config.setMaximumPoolSize(30);
                    config.setConnectionTimeout(5000);
                    config.setMinimumIdle(5);

                    config.setJdbcUrl(url);
                    config.setUsername(username);
                    config.setPassword(password);
                }
            }

            databaseSource = new HikariDataSource(config);
            createTables();
        } catch (java.io.IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException e) {
            plugin.getLogger().severe(Keklist.getTranslations().get("database.driver-missing"));
            Bukkit.getPluginManager().disablePlugin(plugin);
        }
    }


    @NotNull
    public CompletableFuture<Integer> onUpdateAsync(@NotNull @Language("SQL") String update, @Nullable Object... preparedArgs) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = databaseSource.getConnection();
                 PreparedStatement preparedStatement = connection.prepareStatement(update)) {

                if (preparedArgs != null)
                    for (int i = 0; i < preparedArgs.length; i++) {
                        preparedStatement.setObject(i + 1, preparedArgs[i]);
                    }

                return preparedStatement.executeUpdate();
            } catch (SQLException e) {
                plugin.getSLF4JLogger().error("Failed to execute update asynchronously", e);
                return -1;
            }
        }).exceptionally(throwable -> {
            plugin.getSLF4JLogger().error("Failed to execute update asynchronously", throwable);
            return -1;
        }).orTimeout(15, TimeUnit.SECONDS);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Deprecated(since = "1.0.0")
    public void onUpdate(@NotNull @Language("SQL") final String statement, Object... preparedArgs) {
        if (isConnected()) {
            new FutureTask(new Runnable() {
                PreparedStatement preparedStatement;

                public void run() {
                    try (Connection connection = databaseSource.getConnection()) {
                        this.preparedStatement = connection.prepareStatement(statement);

                        for (int i = 0; i < preparedArgs.length; i++) {
                            this.preparedStatement.setObject(i + 1, preparedArgs[i]);
                        }

                        this.preparedStatement.executeUpdate();
                        this.preparedStatement.close();
                    } catch (SQLException throwable) {
                        plugin.getSLF4JLogger().error("Failed to execute update", throwable);
                    }
                }
            }, null).run();
        } else {
            connect();
            onUpdate(statement, preparedArgs);
        }
    }

    @NotNull
    public CompletableFuture<ResultSet> onQueryAsync(@NotNull @Language("SQL") final String query, @Nullable Object... preparedArgs) {
        if (!isConnected()) {
            return CompletableFuture.failedFuture(new SQLException("Not connected"));
        }

        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = databaseSource.getConnection();
                 PreparedStatement ps = connection.prepareStatement(query)) {

                if (preparedArgs != null) {
                    for (int i = 0; i < preparedArgs.length; i++) {
                        ps.setObject(i + 1, preparedArgs[i]);
                    }
                }

                return ps.executeQuery();
            } catch (SQLException e) {
                throw new CompletionException(e);
            }
        }).whenComplete((resultSet, throwable) -> {
            if (throwable != null) {
                plugin.getSLF4JLogger().error("Failed to execute query asynchronously", throwable);
            } else {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    plugin.getSLF4JLogger().error("Failed to close result set", e);
                }
            }
        }).orTimeout(15, TimeUnit.SECONDS);
    }

    @Nullable
    @Deprecated(since = "1.0.0")
    public ResultSet onQuery(@NotNull @Language("SQL") final String query, Object... preparedArgs) {
        if (isConnected()) {
            try (Connection connection = databaseSource.getConnection()) {
                FutureTask<ResultSet> task = new FutureTask<>(new Callable<>() {
                    PreparedStatement ps;

                    public ResultSet call() throws Exception {
                        this.ps = connection.prepareStatement(query);

                        for (int i = 0; i < preparedArgs.length; i++) {
                            this.ps.setObject(i + 1, preparedArgs[i]);
                        }

                        return this.ps.executeQuery();
                    }
                });

                task.run();
                return task.get();
            } catch (Exception e) {
                plugin.getSLF4JLogger().error("Failed to execute query", e);
                return null;
            }
        } else {
            connect();
            return onQuery(query);
        }
    }

    public boolean isConnected() {
        return (databaseSource != null);
    }

    public void disconnect() {
        if (databaseSource != null)
            databaseSource.close();
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
}
