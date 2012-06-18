/**
 * OrbisGIS is a GIS application dedicated to scientific spatial simulation.
 * This cross-platform GIS is developed at French IRSTV institute and is able to
 * manipulate and create vector and raster spatial information.
 *
 * OrbisGIS is distributed under GPL 3 license. It is produced by the "Atelier SIG"
 * team of the IRSTV Institute <http://www.irstv.fr/> CNRS FR 2488.
 *
 * Copyright (C) 2007-1012 IRSTV (FR CNRS 2488)
 *
 * This file is part of OrbisGIS.
 *
 * OrbisGIS is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * OrbisGIS is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * OrbisGIS. If not, see <http://www.gnu.org/licenses/>.
 *
 * For more information, please consult: <http://www.orbisgis.org/>
 * or contact directly:
 * info_at_ orbisgis.org
 */
package org.gdms.h2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.gdms.TestBase;
import org.gdms.data.DataSource;
import org.gdms.data.DataSourceFactory;
import org.gdms.data.db.DBSource;
import org.gdms.data.db.DBSourceCreation;
import org.gdms.data.schema.DefaultMetadata;
import org.gdms.data.schema.Metadata;
import org.gdms.data.types.*;
import org.gdms.driver.h2.H2spatialDriver;
import org.gdms.source.SourceManager;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author alexis
 */
public class H2MetadataTest {

        protected DataSourceFactory dsf;
        protected SourceManager sm;

        @Before
        public void setUp() throws Exception {
                dsf = new DataSourceFactory();
                dsf.setTempDir(TestBase.backupDir.getAbsolutePath());
                dsf.setResultDir(TestBase.backupDir);
                org.gdms.driver.driverManager.DriverManager dm = dsf.getSourceManager().getDriverManager();
                dm.registerDriver(H2spatialDriver.class);
                sm = dsf.getSourceManager();
                sm.removeAll();
        }


        protected DBSource getH2Source(String tableName) {
                return new DBSource(null, -1, "src/test/resources/backup/" + tableName,
                        "sa", "", tableName, "jdbc:h2");
        }
        @Test
        public void testH2ReadString() throws Exception {
                assumeTrue(TestBase.h2Available);
                String tableName;
                tableName = "testh2metadatastring2";
                DBSource source = getH2Source(tableName);
                deleteTable(source);
                executeScript(source, "CREATE TABLE " + tableName
                        + " (id integer primary key, limitedstring varchar(12), "
                        + "unlimitedstring varchar);");
                sm.register("source", source);

                DataSource ds = dsf.getDataSource("source", DataSourceFactory.STATUS_CHECK);
                ds.open();
                Metadata m = ds.getMetadata();
                assertTrue(m.getFieldName(1).equalsIgnoreCase("limitedstring"));
                assertEquals(m.getFieldType(1).getConstraints().length, 0);
                assertTrue(m.getFieldName(2).equalsIgnoreCase("unlimitedstring"));
                assertEquals(m.getFieldType(2).getConstraints().length, 0);
                ds.close();
        }

        @Test
        public void testReadNumericH2() throws Exception {
                assumeTrue(TestBase.h2Available);
                String tableName = "testh2metadatanumeric";
                testReadNumeric(
                        getH2Source(tableName),
                        "CREATE TABLE "
                        + tableName
                        + " (id integer primary key, limitednumeric1 numeric(12),"
                        + " limitednumeric2 numeric(12, 3), "
                        + "\"unlimitedinteger\" int4) ; ");
                sm.removeAll();
        }

        @Test
        public void testWriteStringH2() throws Exception {
                assumeTrue(TestBase.h2Available);
                String tableName = "test_metadata_write_string";
                testWriteString(getH2Source(tableName), 4, -1);
        }

        @Test
        public void testWriteNumericH2() throws Exception {
                assumeTrue(TestBase.h2Available);
                String tableName = "test_metadata_write_string";
                DBSource h2Source = getH2Source(tableName);

                testTypeIO(TypeFactory.createType(Type.BYTE), TypeFactory.createType(Type.BYTE), h2Source);
                testTypeIO(TypeFactory.createType(Type.BYTE,
                        new Constraint[]{new PrecisionConstraint(4)}), TypeFactory.createType(Type.SHORT), h2Source);
                testTypeIO(TypeFactory.createType(Type.SHORT), TypeFactory.createType(Type.SHORT), h2Source);

                testTypeIO(TypeFactory.createType(Type.INT), TypeFactory.createType(Type.INT), h2Source);

                testTypeIO(TypeFactory.createType(Type.LONG), TypeFactory.createType(Type.LONG), h2Source);

                testTypeIO(TypeFactory.createType(Type.SHORT,
                        new Constraint[]{new PrecisionConstraint(5)}), TypeFactory.createType(Type.INT), h2Source);
                testTypeIO(TypeFactory.createType(Type.INT,
                        new Constraint[]{new PrecisionConstraint(14)}), TypeFactory.createType(Type.LONG), h2Source);
                testTypeIO(TypeFactory.createType(Type.INT,
                        new Constraint[]{new PrecisionConstraint(34)}), TypeFactory.createType(Type.DOUBLE), h2Source);

                testTypeIO(TypeFactory.createType(Type.INT, new PrecisionConstraint(6),
                        new LengthConstraint(8)),
                        TypeFactory.createType(Type.INT), h2Source);
        }

        private void executeScript(DBSource dbSource, String statement)
                throws Exception {
                Class.forName("org.postgresql.Driver").newInstance();
                Class.forName("org.h2.Driver").newInstance();
                Class.forName("org.hsqldb.jdbcDriver").newInstance();
                String connectionString = dbSource.getPrefix() + ":";
                if (dbSource.getHost() != null) {
                        connectionString += "//" + dbSource.getHost();

                        if (dbSource.getPort() != -1) {
                                connectionString += (":" + dbSource.getPort());
                        }
                        connectionString += "/";
                }

                connectionString += (dbSource.getDbName());

                Connection c = DriverManager.getConnection(connectionString, dbSource.getUser(), dbSource.getPassword());

                Statement st = c.createStatement();
                st.execute(statement);
                st.close();
                c.close();
        }

        private void deleteTable(DBSource source) {
                String script = "DROP TABLE " + source.getTableName() + ";";
                try {
                        executeScript(source, script);
                } catch (Exception e) {
                }
        }

        private void testTypeIO(Type inType, Type outType, DBSource source)
                throws Exception {
                // Create a metadata with String and a length constraint
                DefaultMetadata metadata = new DefaultMetadata();
                metadata.addField("id", Type.INT, new PrimaryKeyConstraint());
                metadata.addField("field", inType);
                // Create the db source
                deleteTable(source);
                dsf.createDataSource(new DBSourceCreation(source, metadata));
                // read it
                DataSource ds = dsf.getDataSource(source, DataSourceFactory.STATUS_CHECK);
                ds.open();
                Metadata m = ds.getMetadata();
                Type readType = m.getFieldType(1);
                assertEquals(readType.getTypeCode(), outType.getTypeCode());
                Constraint[] readConstraints = readType.getConstraints();
                Constraint[] outConstraints = outType.getConstraints();
                assertEquals(readConstraints.length, outConstraints.length);
                for (int i = 0; i < outConstraints.length; i++) {
                        int constr = readConstraints[i].getConstraintCode();
                        assertEquals(readType.getConstraintValue(constr), outType.getConstraintValue(constr));
                }
                ds.close();
        }

        private void testWriteString(DBSource source, int lengthConstraint,
                int storedConstraint) throws Exception {
                // Create a metadata with String and a length constraint
                DefaultMetadata metadata = new DefaultMetadata();
                metadata.addField("id", Type.INT,
                        new Constraint[]{new PrimaryKeyConstraint()});
                metadata.addField("myLimitedString", Type.STRING,
                        new Constraint[]{new LengthConstraint( lengthConstraint)});
                metadata.addField("myUnlimitedString", Type.STRING);
                // Create the db source
                deleteTable(source);
                dsf.createDataSource(new DBSourceCreation(source, metadata));
                // read it
                DataSource ds = dsf.getDataSource(source, DataSourceFactory.STATUS_CHECK);
                ds.open();
                Metadata m = ds.getMetadata();
                assertTrue(m.getFieldName(1).equalsIgnoreCase("myLimitedString"));
                assertEquals(m.getFieldType(1).getIntConstraint(Constraint.LENGTH), storedConstraint);
                if (storedConstraint == -1) {
                        assertEquals(m.getFieldType(1).getConstraints().length, 0);
                } else {
                        assertEquals(m.getFieldType(1).getConstraints().length, 1);

                }
                assertTrue(m.getFieldName(2).equalsIgnoreCase("myUnlimitedString"));
                assertEquals(m.getFieldType(2).getConstraints().length, 0);
                ds.close();
        }

        private void testReadNumeric(DBSource source, String createSQL)
                throws Exception {
                deleteTable(source);
                executeScript(source, createSQL);
                sm.register("source", source);

                DataSource ds = dsf.getDataSource("source", DataSourceFactory.STATUS_CHECK);
                ds.open();
                Metadata m = ds.getMetadata();
                assertTrue(m.getFieldName(1).equalsIgnoreCase("limitednumeric1"));
                assertEquals(m.getFieldType(1).getIntConstraint(Constraint.PRECISION), 12);
                assertEquals(m.getFieldType(1).getConstraints().length, 1);
                assertTrue(m.getFieldName(2).equalsIgnoreCase("limitednumeric2"));
                assertEquals(m.getFieldType(2).getIntConstraint(Constraint.PRECISION), 12);
                assertEquals(m.getFieldType(2).getIntConstraint(Constraint.SCALE), 3);
                assertEquals(m.getFieldType(2).getConstraints().length, 2);
                assertTrue(m.getFieldName(3).equalsIgnoreCase("unlimitedinteger"));
                assertEquals(m.getFieldType(3).getConstraints().length, 0);
                ds.close();
        }
}
