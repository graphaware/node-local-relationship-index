package com.graphaware.graphhack.config;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.Dynamic;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.setting;

public class IndexConfig implements LoadableConfig {

    /**
     *
     * TODO work out what the default should be
     */
    @Dynamic
    @Description("")
    public static final Setting<Integer> threshold = setting("com.graphaware.nlri.threshold", INTEGER, "100");

}
