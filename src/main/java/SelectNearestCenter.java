import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;


public final class SelectNearestCenter {
    private static PCollection<KV<Integer, Point>> getNearestCentroid(Point p, PCollection<Centroid> centroids) throws Exception {

        PCollection<KV<Integer, Point>> nearestCentroid = centroids.apply(ParDo.of(new DoFn<Centroid, KV<Integer, Point>>() {
            @ProcessElement
            public void processElement(@Element Centroid centroid, OutputReceiver<KV<Integer, Point>> out) {
                // compute distance
                double distance = p.euclideanDistance(p);
                double minDistance = Double.MAX_VALUE;
                int closestCentroidId = -1;
                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.id;
                }
                out.output(KV.of(closestCentroidId, p));
            }
        }));

        return nearestCentroid;
    }
}
