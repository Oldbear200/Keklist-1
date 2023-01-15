package de.hdg.keklist.commandCompletions;

import de.hdg.keklist.database.DB;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.command.TabCompleter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

public class WhitelistCompletor implements @Nullable TabCompleter {
    @Override
    public @Nullable List<String> onTabComplete(@NotNull CommandSender sender, @NotNull Command command, @NotNull String label, @NotNull String[] args) {
        if (args.length == 1) {
            return List.of("add", "remove");
        }else if (args.length == 2) {
            Connection conn = DB.getDB();
            Statement statement = null;
            try {
                statement = conn.createStatement();
                return List.of(statement.executeQuery("SELECT player FROM whitelist").toString());
            }catch (Exception e) {
                e.printStackTrace();
            }
        }

        return null;
    }
}
