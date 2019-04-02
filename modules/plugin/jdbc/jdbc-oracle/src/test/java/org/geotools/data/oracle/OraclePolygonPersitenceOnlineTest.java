package org.geotools.data.oracle;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleStruct;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.oracle.sdo.GeometryConverter;
import org.geotools.jdbc.JDBCDataStoreAPITestSetup;
import org.geotools.jdbc.JDBCTestSupport;
import org.geotools.jdbc.TestData;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class OraclePolygonPersitenceOnlineTest
		extends JDBCTestSupport
{

	GeometryFactory geometryFactory = new GeometryFactory();

	@Override
	protected JDBCDataStoreAPITestSetup createTestSetup() {
		return new EmptyJDBCDataStoreAPITestSetup(new OracleTestSetup());
	}


	/**
	 * Using "optimized rectangle" for storing data is not supported on oracle.
	 * See oracle documentation:
	 *  <ul>
	 *      <li> Oracle 9.2 - https://docs.oracle.com/cd/B10500_01/appdev.920/a96630/sdo_objrelschema.htm#i1005614</li>
	 *      <li> Oracle 11 - https://docs.oracle.com/cd/B28359_01/appdev.111/b28400/sdo_objrelschema.htm#SPATL490</li>
	 *      <li> Oracle 12 - https://docs.oracle.com/database/121/SPATL/sdo_geometry-object-type.htm#SPATL494</li>
	 *      <li> Oracle 19 - https://docs.oracle.com/en/database/oracle/oracle-database/19/spatl/spatial-datatypes-metadata.html#GUID-270AE39D-7B83-46D0-9DD6-E5D99C045021</li>
	 *  </ul>
	 */
	public void testStoringPerfectPolygonsOnOracle12c() throws Exception {
		TestData td = new TestData(0);
		td.build();

		Coordinate first;
		Polygon perfectPolygon = geometryFactory.createPolygon(new Coordinate[] {
				first = new Coordinate(16.88, 4.11),
				new Coordinate(16.88, 4.15),
				new Coordinate(16.89, 4.15),
				new Coordinate(16.89, 4.11),
				first
		});

		td.rectangleLake.setAttribute(td.LAKE_GEOM, perfectPolygon);


		try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
				     dataStore.getFeatureWriterAppend(tname("lake"), Transaction.AUTO_COMMIT)) {
			//inserting new feature
			SimpleFeature feature = writer.next();
			feature.setAttributes(td.rectangleLake.getAttributes());
			writer.write();
		}

		perfectPolygon.setSRID(4326);

		//workaround for ORA-13208 error below
		PreparedStatement ps = dataStore.getDataSource().getConnection().prepareStatement("select * from " + tname("lake") + " where sdo_relate(geom, ?, 'mask=anyinteract querytype=WINDOW') = 'TRUE'");

		OracleConnection oracleConnection = dataStore.getDataSource().getConnection().unwrap(OracleConnection.class);
		GeometryConverter geometryConverter = new GeometryConverter(oracleConnection, geometryFactory);
		OracleStruct sdoStruct = geometryConverter.toSDO(perfectPolygon);
		ps.setObject(1, sdoStruct, Types.STRUCT);

		ResultSet resultSet = ps.executeQuery();
		assertTrue(resultSet.next());


    	/* Oracle throws you this exception because SRID is not set on parameterized bbox geometry in SDO
		Exception (Norwegian locale):
			Caused by: java.sql.SQLException: ORA-29902: feil ved utføring av ODCIIndexStart()-rutine
			ORA-13208: intern feil under evaluering av [window SRID does not match layer SRID]-operatoren
			ORA-06512: ved "MDSYS.SDO_INDEX_METHOD_10I", line 591
		 */

//		try (Transaction t = new DefaultTransaction()) {
//			FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
//			Filter filter = ff.bbox(td.LAKE_GEOM, 16.7887, 4.0862, 16.8796, 4.533, "4326");
//
//			Query q = new Query(tname("lake"), filter);
////			q.setCoordinateSystem(CRS.decode("EPSG:4326"));
//			try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = dataStore.getFeatureReader(q, t)) {
//
//				assertTrue(reader.hasNext());
//				reader.next();
//			}
//		}

	}

}
