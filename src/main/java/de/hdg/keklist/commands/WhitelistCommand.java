package de.hdg.keklist.commands;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import de.hdg.keklist.Keklist;
import de.hdg.keklist.api.events.whitelist.*;
import de.hdg.keklist.util.LanguageUtil;
import de.hdg.keklist.extentions.WebhookManager;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.minimessage.MiniMessage;
import okhttp3.*;
import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.geysermc.floodgate.api.FloodgateApi;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

public class WhitelistCommand extends Command {

    private static final OkHttpClient client = new OkHttpClient();
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().setLenient().create();
    private static final TypeToken<Map<String, String>> token = new TypeToken<>() {
    };

    public WhitelistCommand() {
        super("whitelist");
        setDescription(Keklist.getTranslations().get("whitelist.description"));
        setAliases(List.of("wl"));
        setUsage(Keklist.getTranslations().get("whitelist.usage"));
    }

    @Override
    public boolean execute(@NotNull CommandSender sender, @NotNull String commandLabel, @NotNull String[] args) {
        if (args.length < 2) {
            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("invalid-syntax")));
            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.usage.command")));
            return true;
        }

        try {
            String senderName = sender.getName();
            WhiteListType type;

            if (args[1].matches("^[a-zA-Z0-9_]{2,16}$")) {
                type = WhiteListType.JAVA;
            } else if (args[1].matches("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$")) {
                type = WhiteListType.IPv4;
            } else if (args[1].matches("^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$")) {
                type = WhiteListType.IPv6;
            } else if (args[1].startsWith(Keklist.getInstance().getConfig().getString("floodgate.prefix"))) {
                if (Keklist.getInstance().getFloodgateApi() == null) {
                    sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.invalid-argument")));
                    return true;
                } else
                    type = WhiteListType.BEDROCK;
            } else if (args[1].matches("^((?!-))(xn--)?[a-z0-9][a-z0-9-_]{0,61}[a-z0-9]{0,}\\.?((xn--)?([a-z0-9\\-.]{1,61}|[a-z0-9-]{1,30})\\.?[a-z]{2,})$")) {
                type = WhiteListType.DOMAIN;
            } else {
                sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.invalid-argument")));
                return true;
            }


            switch (args[0]) {
                case "add" -> {
                    if (!sender.hasPermission("keklist.whitelist.add")) {
                        sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("no-permission")));
                        return false;
                    }

                    if (type.equals(WhiteListType.JAVA)) {
                        Request request = new Request.Builder().url("https://api.mojang.com/users/profiles/minecraft/" + args[1]).build();
                        client.newCall(request).enqueue(new WhitelistCommand.UserWhitelistAddCallback(sender, type));
                    } else if (type.equals(WhiteListType.IPv4) || type.equals(WhiteListType.IPv6)) {
                        ResultSet rs = Keklist.getDatabase().onQuery("SELECT * FROM whitelistIp WHERE ip = ?", args[1]);

                        if (!rs.next()) {
                            new IpAddToWhitelistEvent(args[1]).callEvent();
                            Keklist.getDatabase().onUpdate("INSERT INTO whitelistIp (ip, byPlayer, unix) VALUES (?, ?, ?)", args[1], senderName, System.currentTimeMillis());
                            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.added", args[1])));

                            if (Keklist.getWebhookManager() != null)
                                Keklist.getWebhookManager().fireWhitelistEvent(WebhookManager.EVENT_TYPE.WHITELIST_ADD, args[1], senderName, System.currentTimeMillis());

                            if (Keklist.getInstance().getConfig().getBoolean("chat-notify"))
                                Bukkit.broadcast(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.notify.add", args[1], senderName)), "keklist.notify.whitelist");
                        } else
                            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.already-whitelisted", args[1])));

                    } else if (type.equals(WhiteListType.BEDROCK)) {
                        FloodgateApi api = Keklist.getInstance().getFloodgateApi();

                        try {
                            UUID bedrockUUID = api.getUuidFor(args[1].replace(Keklist.getInstance().getConfig().getString("floodgate.prefix"), "")).get();
                            whitelistUser(sender, bedrockUUID, args[1]);
                        } catch (IllegalStateException ex) {

                            if (Keklist.getInstance().getConfig().getString("floodgate.api-key") != null) {
                                Request request = new Request.Builder()
                                        .url("https://mcprofile.io/api/v1/bedrock/gamertag/" + args[1].replace(".", ""))
                                        .header("x-api-key", Keklist.getInstance().getConfig().getString("floodgate.api-key"))
                                        .build();

                                client.newCall(request).enqueue(new WhitelistCommand.UserWhitelistAddCallback(sender, type));
                                return false;
                            } else
                                sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("floodgate.api-key-not-set")));

                        }

                    } else if (type.equals(WhiteListType.DOMAIN)) {
                        ResultSet rs = Keklist.getDatabase().onQuery("SELECT * FROM whitelistDomain WHERE domain = ?", args[1]);

                        if (!rs.next()) {
                            try {
                                InetAddress address = InetAddress.getByName(args[1]);

                                new DomainAddToWhitelistEvent(args[1]).callEvent();
                                Keklist.getDatabase().onUpdate("INSERT INTO whitelistDomain (domain, byPlayer, unix) VALUES (?, ?, ?)", args[1], senderName, System.currentTimeMillis());
                                sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.domain-added", args[1], address.getHostAddress())));

                                if (Keklist.getWebhookManager() != null)
                                    Keklist.getWebhookManager().fireWhitelistEvent(WebhookManager.EVENT_TYPE.WHITELIST_ADD, args[1], senderName, System.currentTimeMillis());

                                if (Keklist.getInstance().getConfig().getBoolean("chat-notify"))
                                    Bukkit.broadcast(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.notify.add", args[1], senderName)), "keklist.notify.whitelist");


                            } catch (UnknownHostException e) {
                                sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.invalid-domain", args[1])));
                                return true;
                            }

                        } else
                            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.already-whitelisted", args[1])));
                    }

                    return true;
                }

                case "remove" -> {
                    if (!sender.hasPermission("keklist.whitelist.remove")) {
                        sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("no-permission")));
                        return false;
                    }

                    if (type.equals(WhiteListType.JAVA) || type.equals(WhiteListType.BEDROCK)) {
                        ResultSet rs = Keklist.getDatabase().onQuery("SELECT * FROM whitelist WHERE name = ?", args[1]);
                        if (rs.next()) {
                            new PlayerRemovedFromWhitelistEvent(args[1]).callEvent();
                            Keklist.getDatabase().onUpdate("DELETE FROM whitelist WHERE name = ?", args[1]);
                            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.removed", args[1])));

                            if (Keklist.getWebhookManager() != null)
                                Keklist.getWebhookManager().fireWhitelistEvent(WebhookManager.EVENT_TYPE.WHITELIST_REMOVE, args[1], senderName, System.currentTimeMillis());

                            if (Keklist.getInstance().getConfig().getBoolean("chat-notify"))
                                Bukkit.broadcast(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.notify.remove", args[1], senderName)), "keklist.notify.whitelist");

                        } else {
                            ResultSet rsUserFix = Keklist.getDatabase().onQuery("SELECT * FROM whitelist WHERE name = ?", args[1] + " (Old Name)");
                            if (rsUserFix.next()) {
                                Keklist.getDatabase().onUpdate("DELETE FROM whitelist WHERE name = ?", args[1] + " (Old Name)");
                                sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.removed", args[1] + " (Old Name)")));

                                if (Keklist.getWebhookManager() != null)
                                    Keklist.getWebhookManager().fireWhitelistEvent(WebhookManager.EVENT_TYPE.WHITELIST_REMOVE, args[1], senderName, System.currentTimeMillis());

                                if (Keklist.getInstance().getConfig().getBoolean("chat-notify"))
                                    Bukkit.broadcast(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.notify.remove", args[1] + "(Old Name)", senderName)), "keklist.notify.whitelist");

                            } else {
                                sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.not-whitelisted", args[1])));
                            }
                        }
                    } else if (type.equals(WhiteListType.IPv4) || type.equals(WhiteListType.IPv6)) {
                        ResultSet rs = Keklist.getDatabase().onQuery("SELECT * FROM whitelistIp WHERE ip = ?", args[1]);
                        if (rs.next()) {
                            new IpRemovedFromWhitelistEvent(args[1]).callEvent();
                            Keklist.getDatabase().onUpdate("DELETE FROM whitelistIp WHERE ip = ?", args[1]);
                            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.removed", args[1])));

                            if (Keklist.getWebhookManager() != null)
                                Keklist.getWebhookManager().fireWhitelistEvent(WebhookManager.EVENT_TYPE.WHITELIST_REMOVE, args[1], senderName, System.currentTimeMillis());

                            if (Keklist.getInstance().getConfig().getBoolean("chat-notify"))
                                Bukkit.broadcast(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.notify.remove", args[1], senderName)), "keklist.notify.whitelist");

                        } else {
                            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.not-whitelisted", args[1])));
                        }
                    } else if (type.equals(WhiteListType.DOMAIN)) {
                        ResultSet rs = Keklist.getDatabase().onQuery("SELECT * FROM whitelistDomain WHERE domain = ?", args[1]);
                        if (rs.next()) {
                            new DomainRemovedFromWhitelistEvent(args[1]).callEvent();
                            Keklist.getDatabase().onUpdate("DELETE FROM whitelistDomain WHERE domain = ?", args[1]);
                            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.removed", args[1])));

                            if (Keklist.getWebhookManager() != null)
                                Keklist.getWebhookManager().fireWhitelistEvent(WebhookManager.EVENT_TYPE.WHITELIST_REMOVE, args[1], senderName, System.currentTimeMillis());

                            if (Keklist.getInstance().getConfig().getBoolean("chat-notify"))
                                Bukkit.broadcast(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.notify.remove", args[1], senderName)), "keklist.notify.whitelist");

                        } else {
                            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.not-whitelisted", args[1])));
                        }
                    }

                    return true;
                }

                case "info" -> {
                    if (!sender.hasPermission("keklist.whitelist.info")) {
                        sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("no-permission")));
                        return false;
                    }

                    if (type.equals(WhitelistCommand.WhiteListType.IPv4) || type.equals(WhitelistCommand.WhiteListType.IPv6)) {
                        ResultSet rs = Keklist.getDatabase().onQuery("SELECT * FROM whitelistIp WHERE ip = ?", args[1]);

                        if (rs.next()) {
                            sendInfo(rs, sender, args[1]);
                        } else
                            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("blacklist.not-blacklisted", args[1])));

                    } else if (type.equals(WhitelistCommand.WhiteListType.JAVA) || type.equals(WhitelistCommand.WhiteListType.BEDROCK)) {
                        ResultSet rs = Keklist.getDatabase().onQuery("SELECT * FROM whitelist WHERE name = ?", args[1]);

                        if (rs.next()) {
                            sendInfo(rs, sender, args[1]);
                        } else {
                            ResultSet rsUserFix = Keklist.getDatabase().onQuery("SELECT * FROM whitelist WHERE name = ?", args[1] + " (Old Name)");
                            if (rsUserFix.next()) {
                                sendInfo(rsUserFix, sender, args[1] + " (Old Name)");
                            } else
                                sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.not-whitelisted", args[1])));
                        }
                    } else if (type.equals(WhitelistCommand.WhiteListType.DOMAIN)) {
                        ResultSet rs = Keklist.getDatabase().onQuery("SELECT * FROM whitelistDomain WHERE domain = ?", args[1]);

                        if (rs.next()) {
                            sendInfo(rs, sender, args[1]);
                        } else
                            sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.not-whitelisted", args[1])));
                    }
                }

                default -> {
                    sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("invalid-syntax")));
                    sender.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.usage.command")));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    private void sendInfo(@NotNull ResultSet resultSet, @NotNull CommandSender sender, @NotNull String entry) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(Keklist.getInstance().getConfig().getString("date-format"));

            String byPlayer = resultSet.getString("byPlayer");
            String unix = sdf.format(resultSet.getLong("unix"));

            LanguageUtil translations = Keklist.getTranslations();
            MiniMessage miniMessage = Keklist.getInstance().getMiniMessage();

            sender.sendMessage(miniMessage.deserialize(translations.get("whitelist.info")));
            sender.sendMessage(miniMessage.deserialize(translations.get("whitelist.info.entry", entry)));
            sender.sendMessage(miniMessage.deserialize(translations.get("whitelist.info.by", byPlayer)));
            sender.sendMessage(miniMessage.deserialize(translations.get("whitelist.info.at", unix)));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void whitelistUser(CommandSender from, @NotNull UUID uuid, String playerName) {
        try {
            ResultSet rs = Keklist.getDatabase().onQuery("SELECT * FROM whitelist WHERE uuid = ?", uuid.toString());
            ResultSet rsUserFix = Keklist.getDatabase().onQuery("SELECT * FROM whitelist WHERE name = ?", playerName);

            if (!rs.next()) {
                if (rsUserFix.next()) {
                    Keklist.getDatabase().onUpdate("UPDATE whitelist SET name = ? WHERE name = ?", playerName + " (Old Name)", playerName);
                }

                Bukkit.getScheduler().runTask(Keklist.getInstance(), () -> new UUIDAddToWhitelistEvent(uuid).callEvent());
                Keklist.getDatabase().onUpdate("INSERT INTO whitelist (uuid, name, byPlayer, unix) VALUES (?, ?, ?, ?)", uuid.toString(), playerName, from.getName(), System.currentTimeMillis());
                from.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.added", playerName)));

                if (Keklist.getWebhookManager() != null)
                    Keklist.getWebhookManager().fireWhitelistEvent(WebhookManager.EVENT_TYPE.WHITELIST_ADD, playerName, from.getName(), System.currentTimeMillis());

                if (Keklist.getInstance().getConfig().getBoolean("chat-notify"))
                    Bukkit.broadcast(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.notify.add", playerName, from.getName())), "keklist.notify.whitelist");

            } else
                from.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("whitelist.already-whitelisted", playerName)));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private class UserWhitelistAddCallback implements Callback {
        private final CommandSender player;
        private final WhiteListType type;

        public UserWhitelistAddCallback(CommandSender player, WhiteListType type) {
            this.player = player;
            this.type = type;
        }

        @Override
        public void onResponse(@NotNull Call call, Response response) throws IOException {

            String body = response.body().string();

            if (checkForGoodResponse(body, type) != null) {
                player.sendMessage(checkForGoodResponse(body, type));
            } else if (response.code() == 429) {
                player.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("http.rate-limit")));
            } else {
                Map<String, String> map = gson.fromJson(body, token);

                String uuid;
                String name;

                if (type.equals(WhiteListType.JAVA)) {
                    uuid = map.get("id");
                    name = map.get("name");
                } else {
                    uuid = map.get("floodgateuid");
                    name = Keklist.getInstance().getConfig().getString("floodgate.prefix") + map.get("gamertag");
                }

                whitelistUser(player, UUID.fromString(uuid.replaceFirst(
                        "(\\p{XDigit}{8})(\\p{XDigit}{4})(\\p{XDigit}{4})(\\p{XDigit}{4})(\\p{XDigit}+)",
                        "$1-$2-$3-$4-$5")), name);
            }
        }

        @Override
        public void onFailure(@NotNull Call call, IOException e) {
            player.sendMessage(Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("http.error")));
            player.sendMessage(Component.text(Keklist.getTranslations().get("http.detail", e.getMessage())));
        }
    }

    @Nullable
    private static Component checkForGoodResponse(@NotNull String response, @NotNull WhiteListType type) {
        JsonElement responseElement = JsonParser.parseString(response);

        if (!responseElement.isJsonNull()) {
            if (type.equals(WhiteListType.JAVA)) {
                if (responseElement.getAsJsonObject().get("error") != null ||
                        !responseElement.getAsJsonObject().has("id") ||
                        !responseElement.getAsJsonObject().has("name")) {
                    return Keklist.getInstance().getMiniMessage().deserialize(Keklist.getTranslations().get("http.not-found", responseElement.getAsJsonObject().get("error").getAsString()));
                }
            } else {
                // 429 when rate limit is reached
                return null; // "The request can't really fail, if for example you give a wrong xuid/gamertag the api just responds with an account not found but will still be a valid request." ~ jens_co
            }

        } else {
            return Component.text(Keklist.getTranslations().get("http.null-response"));
        }

        return null;
    }

    /**
     * Types of whitelist entries
     * <p> USERNAME: Whitelist a player by their username
     * <p> IPv4: Whitelist a player by their IPv4 address
     * <p> IPv6: Whitelist a player by their IPv6 address
     * <p> BEDROCK: Whitelist a player by their Bedrock username
     * <p> DOMAIN: Whitelist a domains which gets resolved to an IP
     */
    private enum WhiteListType {
        IPv4, IPv6, JAVA, BEDROCK, DOMAIN
    }

    @Override
    public @NotNull List<String> tabComplete(@NotNull CommandSender sender, @NotNull String alias, @NotNull String[] args) throws IllegalArgumentException {
        if (args.length < 2) {
            return List.of("add", "remove", "info");
        } else if (args.length == 2) {
            try {
                switch (args[0]) {
                    case "remove", "info" -> {
                        if (!sender.hasPermission("keklist.whitelist.remove")
                                || !sender.hasPermission("keklist.whitelist.info")) return Collections.emptyList();

                        List<String> list = new ArrayList<>();

                        ResultSet rsUser = Keklist.getDatabase().onQuery("SELECT name FROM whitelist");
                        while (rsUser.next()) {
                            list.add(rsUser.getString("name"));
                        }

                        ResultSet rsIp = Keklist.getDatabase().onQuery("SELECT ip FROM whitelistIp");
                        while (rsIp.next()) {
                            list.add(rsIp.getString("ip"));
                        }

                        ResultSet rsDomain = Keklist.getDatabase().onQuery("SELECT domain FROM whitelistDomain");
                        while (rsDomain.next()) {
                            list.add(rsDomain.getString("domain"));
                        }

                        return list;
                    }

                    case "add" -> {
                        if (!sender.hasPermission("keklist.whitelist.add")) return Collections.emptyList();

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
