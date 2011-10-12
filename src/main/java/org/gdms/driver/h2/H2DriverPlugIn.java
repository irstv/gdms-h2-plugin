package org.gdms.driver.h2;

import org.gdms.data.DataSourceFactory;
import org.gdms.plugins.GdmsPlugIn;

public class H2DriverPlugIn implements GdmsPlugIn {

       private DataSourceFactory dsf;

        @Override
        public void load(DataSourceFactory dsf) {
                this.dsf = dsf;
                dsf.getSourceManager().getDriverManager().registerDriver(H2spatialDriver.class);
        }

        @Override
        public void unload() {
                dsf.getSourceManager().getDriverManager().unregisterDriver(H2spatialDriver.DRIVER_NAME);
        }

        @Override
        public String getName() {
                return "H2 Driver plugin";
        }

        @Override
        public String getVersion() {
                return "1.0";
        }
}
