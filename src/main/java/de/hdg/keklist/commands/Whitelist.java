package de.hdg.keklist.commands;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import de.hdg.keklist.Keklist;
import de.hdg.keklist.database.DB;
import okhttp3.OkHttp;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.bukkit.ChatColor;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

public class Whitelist implements CommandExecutor {

    private static final OkHttpClient client = new OkHttpClient();

    @Override
    public boolean onCommand(@NotNull CommandSender sender, @NotNull Command command, @NotNull String label, @NotNull String[] args) {
        if (args.length < 2) {
            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize("<red>Invalider Syntax!"));
            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize("<red>Benutze: /whitelist <add/remove/list> [Spieler/IP]"));
            return true;
        }

        try {
            String senderName = sender.getName();
            WhiteListType type;
            UUID uuid = null;


            if (args[1].matches("^[a-zA-Z0-9_]{2,16}$")) {
                type = WhiteListType.USERNAME;

                ResultSet rs = DB.onQuery("SELECT * FROM whitelist WHERE name = ?", args[1]);

                // User is already whitelisted
                if (rs.next()) {
                    sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize("<red>Dieser User ist bereits gewhitelistet!"));
                    return true;
                }

                Request request = new Request.Builder().url("https://api.mojang.com/users/profiles/minecraft/" + args[1]).build();
                try (Response response = client.newCall(request).execute()) {
                    JsonElement element = JsonParser.parseString(response.body().string());

                    //Mojang API returned an error
                    if (element.getAsJsonObject().get("error") != null) {
                        sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize("<red>Der Spieler existiert nicht! Mehr zum Fehler in der Konsole"));
                        Keklist.getInstance().getLogger().warning("Der Spieler " + args[1] + " existiert nicht!");
                        Keklist.getInstance().getLogger().warning("Details: " + element.getAsJsonObject().get("errorMessage").getAsString());
                        return true;
                    }

                    uuid = UUID.fromString(element.getAsJsonObject().get("id").getAsString().replaceFirst(
                            "(\\p{XDigit}{8})(\\p{XDigit}{4})(\\p{XDigit}{4})(\\p{XDigit}{4})(\\p{XDigit}+)",
                            "$1-$2-$3-$4-$5"));
                }
            } else if (args[1].matches("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$")) {
                type = WhiteListType.IPv4;

                ResultSet rs = DB.onQuery("SELECT * FROM whitelistIp WHERE ip = ?", args[1]);

                // IP is already whitelisted
                if (rs.next()) {
                    sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize("<red>Diese IP ist bereits gewhitelistet!"));
                    return true;
                }
            } else if (args[1].matches("^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$")) {
                //type = WhitelistType.IPv6;
                sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize("<red>IPv6 ist noch nicht unterstützt!"));
                return true;
            } else {
                //TODO : Add floodgate support

                sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize("<red>Ungültige IP oder Username!"));
                return true;
            }


            switch (args[0]) {
                case "add" -> {
                    if (type.equals(WhiteListType.USERNAME)) {
                        DB.onUpdate("INSERT INTO whitelist (, name, by) VALUES (?, ?, ?, ?)", uuid.toString(), args[1], senderName);
                    } else if (type.equals(WhiteListType.IPv4)) {
                        DB.onUpdate("INSERT INTO whitelistIp (ip, by) VALUES (?, ?)", args[1], senderName);
                    } else {/*TODO : May add IPv6*/ }

                    sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize("<green>" + args[1] + " wurde erfolgreich zur Whitelist hinzugefügt!"));
                }

                case "remove" -> {
                    if (type.equals(WhiteListType.USERNAME)) {
                        ResultSet rs = DB.onQuery("SELECT * FROM whitelist WHERE name = ?", args[1]);
                        if (rs.next()) {
                            DB.onUpdate("DELETE FROM whitelist WHERE name = ?", args[1]);
                        } else {
                            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize("<red>Dieser User ist nicht gewhitelistet!"));
                            return true;
                        }
                    } else if (type.equals(WhiteListType.IPv4)) {
                        ResultSet rs = DB.onQuery("SELECT * FROM whitelistIp WHERE ip = ?", args[1]);
                        if (rs.next()) {
                            DB.onUpdate("DELETE FROM whitelistIp WHERE ip = ?", args[1]);
                        } else {
                            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize("<red>Diese IP ist nicht gewhitelistet!"));
                            return true;
                        }
                    } else {/*TODO : May add IPv6*/ }

                    sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize("<green>" + args[1] + " wurde erfolgreich von der Whitelist entfernt!"));
                }

                default -> {
                    sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize("<red>Invalider Syntax!"));
                    sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize("<red>Benutze: /whitelist <add/remove/list> [Spieler/IP]"));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return true;
    }


    /**
     * Types of whitelist entries
     * <p> USERNAME: Whitelist a player by their username
     * <p> IPv4: Whitelist a player by their IPv4 address
     * <p> IPv6: Whitelist a player by their IPv6 address
     */
    private enum WhiteListType {
        IPv4, IPv6, USERNAME
    }
}
