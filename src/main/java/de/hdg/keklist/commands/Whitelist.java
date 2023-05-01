package de.hdg.keklist.commands;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import de.hdg.keklist.Keklist;
import de.hdg.keklist.api.events.whitelist.*;
import de.hdg.keklist.util.WebhookManager;
import net.kyori.adventure.text.Component;
import okhttp3.*;
import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.geysermc.floodgate.api.FloodgateApi;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class Whitelist extends Command {

    private static final OkHttpClient client = new OkHttpClient();
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().setLenient().create();
    private static final TypeToken<Map<String, String>> token = new TypeToken<>() {
    };

    public Whitelist() {
        super("whitelist");
        setPermission("keklist.whitelist");
        setDescription(Keklist.getLanguage().get("whitelist.description"));
        setAliases(List.of("wl"));
        setUsage(Keklist.getLanguage().get("whitelist.usage"));
    }

    @Override
    public boolean execute(@NotNull CommandSender sender, @NotNull String commandLabel, @NotNull String[] args) {
        if (args.length < 2) {
            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getLanguage().get("invalid-syntax")));
            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getLanguage().get("whitelist.usage.command")));
            return true;
        }

        try {
            String senderName = sender.getName();
            WhiteListType type;
            UUID bedrockUUID = null;

            if (args[1].matches("^[a-zA-Z0-9_]{2,16}$")) {
                type = WhiteListType.JAVA;
            } else if (args[1].matches("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$")) {
                type = WhiteListType.IPv4;
            } else if (args[1].matches("^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$")) {
                type = WhiteListType.IPv6;
            } else {
                if (Keklist.getInstance().getFloodgateApi() != null) {
                    if (args[1].startsWith(Keklist.getInstance().getConfig().getString("floodgate.prefix"))) {
                        FloodgateApi api = Keklist.getInstance().getFloodgateApi();

                        bedrockUUID = api.getUuidFor(args[1].replace(Keklist.getInstance().getConfig().getString("floodgate.prefix"), "")).get();
                        type = WhiteListType.BEDROCK;
                    } else {
                        sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getLanguage().get("whitelist.invalid-argument")));
                        return true;
                    }
                } else {
                    sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getLanguage().get("whitelist.invalid-argument")));
                    return true;
                }
            }


            switch (args[0]) {
                case "add" -> {
                    if (type.equals(WhiteListType.JAVA)) {
                        Request request = new Request.Builder().url("https://api.mojang.com/users/profiles/minecraft/" + args[1]).build();
                        client.newCall(request).enqueue(new Whitelist.UserWhitelistAddCallback(sender));
                    } else if (type.equals(WhiteListType.IPv4) || type.equals(WhiteListType.IPv6)) {
                        ResultSet rs = Keklist.getDatabase().onQuery("SELECT * FROM whitelistIp WHERE ip = ?", args[1]);

                        if (!rs.next()) {
                            new IpAddToWhitelistEvent(args[1]).callEvent();
                            Keklist.getDatabase().onUpdate("INSERT INTO whitelistIp (ip, byPlayer, unix) VALUES (?, ?, ?)", args[1], senderName, System.currentTimeMillis());
                            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getLanguage().get("whitelist.added", args[1])));

                            if (Keklist.getWebhookManager() != null)
                                Keklist.getWebhookManager().fireWhitelistEvent(WebhookManager.EVENT_TYPE.WHITELIST_ADD, args[1], senderName, System.currentTimeMillis());
                        } else
                            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getLanguage().get("whitelist.already-whitelisted", args[1])));

                    } else if (type.equals(WhiteListType.BEDROCK)) {
                        whitelistUser(sender, bedrockUUID, senderName);
                    }

                    return true;
                }

                case "remove" -> {
                    if (type.equals(WhiteListType.JAVA) || type.equals(WhiteListType.BEDROCK)) {
                        ResultSet rs = Keklist.getDatabase().onQuery("SELECT * FROM whitelist WHERE name = ?", args[1]);
                        if (rs.next()) {
                            new PlayerRemovedFromWhitelistEvent(args[1]).callEvent();
                            Keklist.getDatabase().onUpdate("DELETE FROM whitelist WHERE name = ?", args[1]);
                            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getLanguage().get("whitelist.removed", args[1])));

                            if (Keklist.getWebhookManager() != null)
                                Keklist.getWebhookManager().fireWhitelistEvent(WebhookManager.EVENT_TYPE.WHITELIST_REMOVE, args[1], senderName, System.currentTimeMillis());

                        } else {
                            ResultSet rsUserFix = Keklist.getDatabase().onQuery("SELECT * FROM whitelist WHERE name = ?", args[1] + " (Old Name)");
                            if (rsUserFix.next()) {
                                Keklist.getDatabase().onUpdate("DELETE FROM whitelist WHERE name = ?", args[1] + " (Old Name)");
                                sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getLanguage().get("whitelist.removed", args[1] + " (Old Name)")));

                                if (Keklist.getWebhookManager() != null)
                                    Keklist.getWebhookManager().fireWhitelistEvent(WebhookManager.EVENT_TYPE.WHITELIST_REMOVE, args[1], senderName, System.currentTimeMillis());

                            } else {
                                sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getLanguage().get("whitelist.not-whitelisted", args[1])));
                            }
                        }
                    } else if (type.equals(WhiteListType.IPv4) || type.equals(WhiteListType.IPv6)) {
                        ResultSet rs = Keklist.getDatabase().onQuery("SELECT * FROM whitelistIp WHERE ip = ?", args[1]);
                        if (rs.next()) {
                            new IpRemovedFromWhitelistEvent(args[1]).callEvent();
                            Keklist.getDatabase().onUpdate("DELETE FROM whitelistIp WHERE ip = ?", args[1]);
                            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getLanguage().get("whitelist.removed", args[1])));

                            if (Keklist.getWebhookManager() != null)
                                Keklist.getWebhookManager().fireWhitelistEvent(WebhookManager.EVENT_TYPE.WHITELIST_REMOVE, args[1], senderName, System.currentTimeMillis());

                        } else {
                            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getLanguage().get("whitelist.not-whitelisted", args[1])));
                        }
                    }

                    return true;
                }

                default -> {
                    sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getLanguage().get("invalid-syntax")));
                    sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getLanguage().get("whitelist.usage.command")));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    private void whitelistUser(CommandSender from, UUID uuid, String playerName) {
        try {
            ResultSet rs = Keklist.getDatabase().onQuery("SELECT * FROM whitelist WHERE uuid = ?", uuid.toString());
            ResultSet rsUserFix = Keklist.getDatabase().onQuery("SELECT * FROM whitelist WHERE name = ?", playerName);

            if (!rs.next()) {
                if (rsUserFix.next()) {
                    Keklist.getDatabase().onUpdate("UPDATE whitelist SET name = ? WHERE name = ?", playerName + " (Old Name)", playerName);
                }

                Bukkit.getScheduler().runTask(Keklist.getInstance(), () -> new UUIDAddToWhitelistEvent(uuid).callEvent());
                Keklist.getDatabase().onUpdate("INSERT INTO whitelist (uuid, name, byPlayer, unix) VALUES (?, ?, ?, ?)", uuid.toString(), playerName, from.getName(), System.currentTimeMillis());
                from.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getLanguage().get("whitelist.added", playerName)));

                if (Keklist.getWebhookManager() != null)
                    Keklist.getWebhookManager().fireWhitelistEvent(WebhookManager.EVENT_TYPE.WHITELIST_ADD, playerName, from.getName(), System.currentTimeMillis());

            } else
                from.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getLanguage().get("whitelist.already-whitelisted", playerName)));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private class UserWhitelistAddCallback implements Callback {
        private final CommandSender player;

        public UserWhitelistAddCallback(CommandSender player) {
            this.player = player;
        }

        @Override
        public void onResponse(Call call, Response response) throws IOException {

            String body = response.body().string();
            if (checkForGoodResponse(body) != null) {
                player.sendMessage(checkForGoodResponse(body));
            } else {
                Map<String, String> map = gson.fromJson(body, token);
                String uuid = map.get("id");
                String name = map.get("name");

                whitelistUser(player, UUID.fromString(uuid.replaceFirst(
                        "(\\p{XDigit}{8})(\\p{XDigit}{4})(\\p{XDigit}{4})(\\p{XDigit}{4})(\\p{XDigit}+)",
                        "$1-$2-$3-$4-$5")), name);
            }
        }

        @Override
        public void onFailure(Call call, IOException e) {
            player.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getLanguage().get("http.error")));
            player.sendMessage(Component.text(Keklist.getLanguage().get("http.detail", e.getMessage())));
        }
    }

    private Component checkForGoodResponse(String response) {
        JsonElement element = JsonParser.parseString(response);

        if (!element.isJsonNull()) {
            if (element.getAsJsonObject().get("error") != null) {
                return Keklist.getInstance().getMiniMessage().deserialize(Keklist.getLanguage().get("http.not-found", element.getAsJsonObject().get("error").getAsString()));
            }
        } else {
            return Component.text(Keklist.getLanguage().get("http.null-response"));
        }

        return null;
    }

    /**
     * Types of whitelist entries
     * <p> USERNAME: Whitelist a player by their username
     * <p> IPv4: Whitelist a player by their IPv4 address
     * <p> IPv6: Whitelist a player by their IPv6 address
     */
    private enum WhiteListType {
        IPv4, IPv6, JAVA, BEDROCK
    }

    @Override
    public @NotNull List<String> tabComplete(@NotNull CommandSender sender, @NotNull String alias, @NotNull String[] args) throws IllegalArgumentException {
        if (args.length < 2) {
            return List.of("add", "remove");
        } else if (args.length == 2) {
            try {
                switch (args[0]) {
                    case "remove" -> {
                        List<String> list = new ArrayList<>();

                        ResultSet rsUser = Keklist.getDatabase().onQuery("SELECT name FROM whitelist");
                        while (rsUser.next()) {
                            list.add(rsUser.getString("name"));
                        }

                        ResultSet rsIp = Keklist.getDatabase().onQuery("SELECT ip FROM whitelistIp");
                        while (rsIp.next()) {
                            list.add(rsIp.getString("ip"));
                        }

                        return list;
                    }

                    case "add" -> {
                        List<String> completions = new ArrayList<>();

                        Bukkit.getOnlinePlayers().forEach(player -> {
                            ResultSet rs = Keklist.getDatabase().onQuery("SELECT * FROM whitelist WHERE uuid = ?", player.getUniqueId().toString());
                            try {
                                if (!rs.next())
                                    completions.add(player.getName());
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        });

                        Bukkit.getOnlinePlayers().forEach(player -> {
                            ResultSet rs = Keklist.getDatabase().onQuery("SELECT * FROM whitelistIp WHERE ip = ?", player.getAddress().getAddress().getHostAddress());
                            try {
                                if (!rs.next())
                                    completions.add(player.getAddress().getAddress().getHostAddress() + "(" + player.getName() + ")");
                                ;
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        });

                        return completions;
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        return Collections.emptyList();
    }
}
