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

import uq.fs.DataStatisticsService;
import uq.fs.DataConverter;
import uq.fs.FileReader;
import uq.fs.HDFSFileService;
import uq.fs.PivotsService;
import uq.fs.TachyonFileService;
import uq.spark.exp.BenchmarkTest;
import uq.spark.exp.PerformanceTest;
import uq.spark.index.Page;
import uq.spark.index.PageIndex;
import uq.spark.index.PageIndexSet;
import uq.spark.index.PagesPartitioningModule;
import uq.spark.index.TrajectoryCollector;
import uq.spark.index.TrajectoryTrackTable;
import uq.spark.index.VoronoiDiagram;
import uq.spark.index.VoronoiPagesRDD;
import uq.spark.query.CrossQueryCalculator;
import uq.spark.query.NearNeighbor;
import uq.spark.query.NearestNeighborQueryCalculator;
import uq.spark.query.NeighborComparator;
import uq.spark.query.QueryProcessingModule;
import uq.spark.query.SelectObject;
import uq.spark.query.SelectionQueryCalculator;
import uq.spatial.Box;
import uq.spatial.Circle;
import uq.spatial.Point;
import uq.spatial.PointComparator;
import uq.spatial.STBox;
import uq.spatial.TimeComparator;
import uq.spatial.Trajectory;
import uq.spatial.TrajectoryRTree;
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
import uq.spatial.distance.EDwPDistanceCalculator;
import uq.spatial.distance.EuclideanDistanceCalculator; 
import uq.spatial.distance.HaversineDistanceCalculator;
import uq.spatial.distance.PointDistanceCalculator; 
import uq.spatial.distance.TrajectoryDistanceCalculator;
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
public class KryoClassRegistrator implements KryoRegistrator{
	/**
	 * Register classes for serialization
	 */
	@SuppressWarnings("rawtypes")
	public void registerClasses(Kryo kryo) {
		// spark
		kryo.register(Logger.class, new FieldSerializer(kryo, Logger.class));
		
		// partitioning
		kryo.register(PagesPartitioningModule.class, new FieldSerializer(kryo, PagesPartitioningModule.class));
		kryo.register(VoronoiPagesRDD.class, new FieldSerializer(kryo, VoronoiPagesRDD.class));
		kryo.register(TrajectoryTrackTable.class, new FieldSerializer(kryo, TrajectoryTrackTable.class));
		kryo.register(VoronoiDiagram.class, new FieldSerializer(kryo, VoronoiDiagram.class));
		kryo.register(PageIndex.class, new FieldSerializer(kryo, PageIndex.class));
		kryo.register(Page.class, new FieldSerializer(kryo, Page.class));
		kryo.register(PageIndexSet.class, new FieldSerializer(kryo, PageIndexSet.class));
		kryo.register(TrajectoryCollector.class, new FieldSerializer(kryo, TrajectoryCollector.class));
		
		// fs
		kryo.register(HDFSFileService.class, new FieldSerializer(kryo, HDFSFileService.class));
		kryo.register(DataConverter.class, new FieldSerializer(kryo, DataConverter.class));
		kryo.register(FileReader.class, new FieldSerializer(kryo, FileReader.class));
		kryo.register(DataStatisticsService.class, new FieldSerializer(kryo, DataStatisticsService.class));
		kryo.register(TachyonFileService.class, new FieldSerializer(kryo, TachyonFileService.class));
		kryo.register(PivotsService.class, new FieldSerializer(kryo, PivotsService.class));
		
		// spatial obj
		kryo.register(Box.class, new FieldSerializer(kryo, Box.class));
		kryo.register(Point.class, new FieldSerializer(kryo, Point.class));
		kryo.register(Circle.class, new FieldSerializer(kryo, Circle.class));
		kryo.register(Trajectory.class, new FieldSerializer(kryo, Trajectory.class));
		kryo.register(PointComparator.class, new FieldSerializer(kryo, PointComparator.class));
		kryo.register(TimeComparator.class, new FieldSerializer(kryo, TimeComparator.class));
		kryo.register(TrajectoryRTree.class, new FieldSerializer(kryo, TrajectoryRTree.class));
		kryo.register(STBox.class, new FieldSerializer(kryo, STBox.class));
		
		// query
		kryo.register(QueryProcessingModule.class, new FieldSerializer(kryo, QueryProcessingModule.class));	
		kryo.register(SelectionQueryCalculator.class, new FieldSerializer(kryo, SelectionQueryCalculator.class));
		kryo.register(CrossQueryCalculator.class, new FieldSerializer(kryo, CrossQueryCalculator.class));
		kryo.register(NearestNeighborQueryCalculator.class, new FieldSerializer(kryo, NearestNeighborQueryCalculator.class));
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
		kryo.register(PointDistanceCalculator.class, new FieldSerializer(kryo, PointDistanceCalculator.class));
		kryo.register(TrajectoryDistanceCalculator.class, new FieldSerializer(kryo, TrajectoryDistanceCalculator.class));
		kryo.register(EDwPDistanceCalculator.class, new FieldSerializer(kryo, EDwPDistanceCalculator.class));
		kryo.register(EuclideanDistanceCalculator.class, new FieldSerializer(kryo, EuclideanDistanceCalculator.class));
		kryo.register(HaversineDistanceCalculator.class, new FieldSerializer(kryo, HaversineDistanceCalculator.class));
		
		// transformation
		kryo.register(ProjectionTransformation.class, new FieldSerializer(kryo, ProjectionTransformation.class));
		
		// delaunay
		kryo.register(Triangle.class, new FieldSerializer(kryo, Triangle.class));
		kryo.register(DelaunayTriangulation.class, new FieldSerializer(kryo, DelaunayTriangulation.class));
		kryo.register(TriangleEdge.class, new FieldSerializer(kryo, TriangleEdge.class));
		
		// experiments
		kryo.register(BenchmarkTest.class, new FieldSerializer(kryo, BenchmarkTest.class));
		kryo.register(PerformanceTest.class, new FieldSerializer(kryo, PerformanceTest.class));
		
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
