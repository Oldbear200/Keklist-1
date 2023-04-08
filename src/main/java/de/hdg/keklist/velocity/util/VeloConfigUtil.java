package de.hdg.keklist.velocity.util;

import de.hdg.keklist.velocity.KeklistVelocity;
import lombok.Getter;
import lombok.SneakyThrows;
import ninja.leaping.configurate.ConfigurationNode;
import ninja.leaping.configurate.yaml.YAMLConfigurationLoader;
import org.jetbrains.annotations.NotNull;
import org.yaml.snakeyaml.DumperOptions;

import java.io.IOException;
import java.nio.file.Path;

public class VeloConfigUtil {

    private YAMLConfigurationLoader configLoader;
    private @Getter Path configDirectory;

    public VeloConfigUtil(@NotNull Path directory, @NotNull String fileName) {
        configLoader = YAMLConfigurationLoader.builder().setPath(directory.resolve(fileName)).build();

        try {
            if (!directory.toFile().exists()) {
                directory.toFile().mkdirs();
                directory.resolve(fileName).toFile().createNewFile();
            }

            this.configLoader = YAMLConfigurationLoader.builder().setPath(directory.resolve("config.yml")).setFlowStyle(DumperOptions.FlowStyle.BLOCK).build();
            this.configDirectory = directory;

            generateConfig();
        } catch (IOException exception) {
            exception.printStackTrace();
            KeklistVelocity.getInstance().getLogger().error("Error while creating config file! Please report this to the developer!");
        }
    }

    @SneakyThrows(IOException.class)
    public Object getOption(Object defaultValue, String path){
        return configLoader.load().getNode(path).getValue()!=null?configLoader.load().getNode(path).getValue():defaultValue;
    }

    public void setValue(Object value, String path) throws IOException {
        ConfigurationNode node = configLoader.load();
        node.getNode(path).setValue(value);
        configLoader.save(node);
    }

    private void generateConfig() throws IOException {
        KeklistVelocity.getInstance().getLogger().info("Generating config...");

        final ConfigurationNode conf = configLoader.load();

        conf.getNode("limbo").getNode("enabled").setValue(true);
        conf.getNode("limbo").getNode("map").setValue(true);
        conf.getNode("limbo").getNode("file").setValue("limbo.nbt");
        configLoader.save(conf);

        KeklistVelocity.getInstance().getLogger().info("Config generated!");
    }

}