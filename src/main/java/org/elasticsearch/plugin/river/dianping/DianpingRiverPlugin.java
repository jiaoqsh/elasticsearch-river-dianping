

package org.elasticsearch.plugin.river.dianping;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.dianping.DianpingRiverModule;

/**
 * DianpingRiver Plugin
 */
public class DianpingRiverPlugin extends AbstractPlugin {

    @Inject
    public DianpingRiverPlugin() {
    }

    @Override
    public String name() {
        return "river-dianping";
    }

    @Override
    public String description() {
        return "River DianPing Plugin";
    }

    public void onModule(RiversModule module) {
        module.registerRiver("dianping", DianpingRiverModule.class);
    }
}
