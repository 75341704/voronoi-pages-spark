package uq.spark;

import java.util.ArrayList; 
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList; 

import org.apache.spark.serializer.KryoRegistrator; 

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.MapSerializer;

import uq.exp.ExperimentsService;
import uq.exp.STObject;
import uq.exp.TimeWindow;
import uq.fs.DatasetStatisticsService;
import uq.fs.FileToObjectRDDService;
import uq.fs.HDFSFileService;
import uq.spark.indexing.Page;
import uq.spark.indexing.PageIndex;
import uq.spark.indexing.PartitioningIndexingService;
import uq.spark.indexing.PivotService;
import uq.spark.indexing.TrajectoryTrackTable;
import uq.spark.indexing.VoronoiDiagram;
import uq.spark.indexing.VoronoiPagesRDD;
import uq.spark.query.CrossQuery;
import uq.spark.query.NearNeighbor;
import uq.spark.query.NearestNeighborQuery;
import uq.spark.query.NeighborComparator;
import uq.spark.query.QueryProcessingService;
import uq.spark.query.SelectObject;
import uq.spark.query.SelectionQuery;
import uq.spatial.Box;
import uq.spatial.Circle;
import uq.spatial.Point;
import uq.spatial.PointComparator;
import uq.spatial.TimeComparator;
import uq.spatial.Trajectory;
import uq.spatial.clustering.Cluster;
import uq.spatial.clustering.ClusterPoint;
import uq.spatial.clustering.DBSCAN;
import uq.spatial.clustering.KMeansSpark;
import uq.spatial.clustering.Medoid;
import uq.spatial.clustering.MedoidComparator;
import uq.spatial.clustering.PartitioningAroundMedoids;
import uq.spatial.delaunay.DelaunayTriangulation;
import uq.spatial.delaunay.Triangle;
import uq.spatial.delaunay.TriangleEdge;
import uq.spatial.distance.DistanceService;
import uq.spatial.distance.EuclideanDistanceCalculator;
import uq.spatial.distance.HaversineDistanceCalculator;
import uq.spatial.transformation.ProjectionTransformation;
import uq.spatial.voronoi.VoronoiDiagramGenerator;
import uq.spatial.voronoi.VoronoiEdge;
import uq.spatial.voronoi.VoronoiPolygon;

/**
 * Register classes for Kryo serialization.
 * 
 * @author uqdalves
 *
 */
public class ClassRegistrator implements KryoRegistrator{
	/**
	 * Register classes for serialization
	 */
	@SuppressWarnings("rawtypes")
	public void registerClasses(Kryo kryo) {
		
		// partitioning
		kryo.register(PartitioningIndexingService.class, new FieldSerializer(kryo, PartitioningIndexingService.class));
		kryo.register(VoronoiPagesRDD.class, new FieldSerializer(kryo, VoronoiPagesRDD.class));
		kryo.register(TrajectoryTrackTable.class, new FieldSerializer(kryo, TrajectoryTrackTable.class));
		kryo.register(VoronoiDiagram.class, new FieldSerializer(kryo, VoronoiDiagram.class));
		kryo.register(PageIndex.class, new FieldSerializer(kryo, PageIndex.class));
		kryo.register(Page.class, new FieldSerializer(kryo, Page.class));
		kryo.register(PivotService.class, new FieldSerializer(kryo, PivotService.class));
		
		// fs
		kryo.register(HDFSFileService.class, new FieldSerializer(kryo, HDFSFileService.class));
		kryo.register(FileToObjectRDDService.class, new FieldSerializer(kryo, FileToObjectRDDService.class));
		kryo.register(DatasetStatisticsService.class, new FieldSerializer(kryo, DatasetStatisticsService.class));

		// spatial obj
		kryo.register(Box.class, new FieldSerializer(kryo, Box.class));
		kryo.register(Point.class, new FieldSerializer(kryo, Point.class));
		kryo.register(Circle.class, new FieldSerializer(kryo, Circle.class));
		kryo.register(Trajectory.class, new FieldSerializer(kryo, Trajectory.class));
		kryo.register(PointComparator.class, new FieldSerializer(kryo, PointComparator.class));
		kryo.register(TimeComparator.class, new FieldSerializer(kryo, TimeComparator.class));
		
		// query
		kryo.register(QueryProcessingService.class, new FieldSerializer(kryo, QueryProcessingService.class));	
		kryo.register(SelectionQuery.class, new FieldSerializer(kryo, SelectionQuery.class));
		kryo.register(CrossQuery.class, new FieldSerializer(kryo, CrossQuery.class));
		kryo.register(NearestNeighborQuery.class, new FieldSerializer(kryo, NearestNeighborQuery.class));
		kryo.register(NeighborComparator.class, new FieldSerializer(kryo, NeighborComparator.class));
		kryo.register(NearNeighbor.class, new FieldSerializer(kryo, NearNeighbor.class));
		kryo.register(SelectObject.class, new FieldSerializer(kryo, SelectObject.class));

		// util
		kryo.register(ArrayList.class, new CollectionSerializer());
		kryo.register(LinkedList.class, new CollectionSerializer());
		kryo.register(HashMap.class, new MapSerializer());
		kryo.register(HashSet.class, new CollectionSerializer());

		// voronoi
		kryo.register(VoronoiDiagramGenerator.class, new FieldSerializer(kryo, VoronoiDiagramGenerator.class));
		kryo.register(VoronoiEdge.class, new FieldSerializer(kryo, VoronoiEdge.class));
		kryo.register(VoronoiPolygon.class, new FieldSerializer(kryo, VoronoiPolygon.class));
		
		// clustering
		kryo.register(KMeansSpark.class, new FieldSerializer(kryo, KMeansSpark.class));
		kryo.register(DBSCAN.class, new FieldSerializer(kryo, DBSCAN.class));
		kryo.register(Cluster.class, new FieldSerializer(kryo, Cluster.class));
		kryo.register(ClusterPoint.class, new FieldSerializer(kryo, ClusterPoint.class));
		kryo.register(Medoid.class, new FieldSerializer(kryo, Medoid.class));
		kryo.register(MedoidComparator.class, new FieldSerializer(kryo, MedoidComparator.class));
		kryo.register(PartitioningAroundMedoids.class, new FieldSerializer(kryo, PartitioningAroundMedoids.class));
		
		// distance
		kryo.register(DistanceService.class, new FieldSerializer(kryo, DistanceService.class));
		kryo.register(EuclideanDistanceCalculator.class, new FieldSerializer(kryo, EuclideanDistanceCalculator.class));
		kryo.register(HaversineDistanceCalculator.class, new FieldSerializer(kryo, HaversineDistanceCalculator.class));
		
		// transformation
		kryo.register(ProjectionTransformation.class, new FieldSerializer(kryo, ProjectionTransformation.class));
		
		// delaunay
		kryo.register(Triangle.class, new FieldSerializer(kryo, Triangle.class));
		kryo.register(DelaunayTriangulation.class, new FieldSerializer(kryo, DelaunayTriangulation.class));
		kryo.register(TriangleEdge.class, new FieldSerializer(kryo, TriangleEdge.class));
		
		// experiments
		kryo.register(ExperimentsService.class, new FieldSerializer(kryo, ExperimentsService.class));
		kryo.register(TimeWindow.class, new FieldSerializer(kryo, TimeWindow.class));
		kryo.register(STObject.class, new FieldSerializer(kryo, STObject.class));
		
		/*
		kryo.register(Color.class, new Serializer() {
	        public void writeObjectData (ByteBuffer buffer, Object object) {
	                buffer.putInt(((Color)object).getRGB());
	        }
	        public Color readObjectData (ByteBuffer buffer, Class type) {
	                return new Color(buffer.getInt());
	        }
		});*/
	}
	
}
