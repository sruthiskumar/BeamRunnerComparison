import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;
import java.util.Comparator;

public class KMeans {
    public static void main1(String[] args) {


        // Create a PipelineOptions object. This object lets us set various execution
        // options for our pipeline, such as the runner we wish to use.
        PipelineOptionsFactory.register(CustomOptions.class);
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(CustomOptions.class);
        int clusters = ((CustomOptions) options).getclusters();
        int iterations = ((CustomOptions) options).getIterations();

        System.out.println(options.getRunner().getName());

        // Create the Pipeline object with the options we defined above
        Pipeline pipeline = Pipeline.create(options);


        // This example reads a public data set consisting of cell tower data of
        // Sweden which is provided by Opencellid.
        PCollection<String> dataFromCsv = pipeline.apply("Read from CSV",
                TextIO.read().from("240.csv"));


        PCollection<Data> data = dataFromCsv.apply(ParDo.of(new DoFn<String, Data>() {
            private static final long serialVersionUID = 1L;

            @ProcessElement
            public void processElement(ProcessContext c) {
                String[] splitVal = c.element().split(",");
                Data d = new Data(splitVal[0].trim(), Long.valueOf(splitVal[1].trim()), Long.valueOf(splitVal[2].trim()),
                        Long.valueOf(splitVal[3].trim()), Integer.valueOf(splitVal[4].trim()), Long.valueOf(splitVal[5].trim()),
                        Double.valueOf(splitVal[6].trim()), Double.valueOf(splitVal[7].trim()), Long.valueOf(splitVal[8].trim()),
                        Long.valueOf(splitVal[9].trim()), Long.valueOf(splitVal[10].trim()), Long.valueOf(splitVal[11].trim()),
                        Long.valueOf(splitVal[12].trim()), Long.valueOf(splitVal[13].trim()));
                c.output(d);
                System.out.println(d.lat + " " +d.lon);
            }
        }));


        PCollection<Point> points = getPointDataSet(data);
        PCollection<Centroid> centroids = getCentroidDataSet(data, clusters);

        int loop = 0;
        while (loop < iterations) {
            loop++;
            System.out.println("Iteration no " + loop);
//
            PCollection<Point> newCentroids = points.apply(
                    ParDo.of(new DoFn<Point, Point>() {
                                 @ProcessElement
                                 public void processElement(@Element Point point, OutputReceiver<Point> out) {

                                     PCollection<Point> nearestCentriods = centroids.apply(ParDo.of(new DoFn<Point, Point>() {
                                         @ProcessElement
                                         public void processElement(@Element Centroid centroid, OutputReceiver<Point> out1) {
                                             double distance = point.euclideanDistance(centroid);
                                             // update the distance and centriod ID
                                             if (distance < point.distance) {
                                                 point.distance = distance;
                                                 point.centriodId = centroid.id;
                                             }
                                         }
                                     }));

                                     out.output(point);
                                 }
                             }
                    ));


            //we have points with centriod ID and smallest distance
            PCollection<KV<Integer, Point>> kv = newCentroids.apply(
                    MapElements.
                            into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptor.of(Point.class)))
                            .via((Point p) -> KV.of(p.centriodId, p))
            );

            PCollection<KV<Integer, Iterable<Point>>> centriodIdToPoints = kv.apply(GroupByKey.<Integer, Point>create());

            PCollection<Centroid> results =
                    centriodIdToPoints.apply(ParDo.of(new DoFn<KV<Integer, Iterable<Point>>, Centroid>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            Integer centriodId = c.element().getKey();
                            Iterable<Point> points = c.element().getValue();
                            int length = 0;
                            double latsum = 0.0, longsum = 0.0;
                            for (Point p : points) {
                                length += 1;
                                latsum += p.lat;
                                longsum += p.lon;
//                            p.centriodId =-1;
//                            p.distance = Double.MAX_VALUE;
                            }
                            System.out.println(length);
                            Point p = new Point(latsum / length * 1.0, longsum / length * 1.0);
                            c.output(new Centroid(centriodId, p));
                            //
                            // ... process all docs having that url ...
                        }
                    }));
        }

        //We have found the new centriods, now we need to align the points accroding to new centriods
        PCollection<Point> clussteredPoints = points.apply(
                ParDo.of(new DoFn<Point, Point>() {
                             @ProcessElement
                             public void processElement(@Element Point point, OutputReceiver<Point> out) {

                                 PCollection<Point> nearestCentriods = centroids.apply(ParDo.of(new DoFn<Point, Point>() {
                                     @ProcessElement
                                     public void processElement(@Element Centroid centroid, OutputReceiver<Point> out1) {
                                         double distance = point.euclideanDistance(centroid);
                                         // update the distance and centriod ID
                                         if (distance < point.distance) {
                                             point.distance = distance;
                                             point.centriodId = centroid.id;
                                         }
                                     }
                                 }));

                                 out.output(point);
                             }
                         }
                ));


//        clussteredPoints.apply(TextIO.<Point>writeCustomType(new FormatEvent())
//                        .to(new SerializableFunction<Point, DefaultFilenamePolicy.Params>() {
//                            public String apply(Point value) {
//                                return new DefaultFilenamePolicy.Params().withBaseFilename("output.csv");
//                            }
//                        }),
//                new DefaultFilenamePolicy.Params().withBaseFilename("output.csv");
//
        PCollection<String> stringPoints = points.apply(ParDo.of(new DoFn<Point, String>() {
            @ProcessElement
            public void processElement(@Element Point point, OutputReceiver<String> out) {
             out.output(point.toString());
            }
        }));


        stringPoints.apply(TextIO.write().to("Koutput"));

//        points.apply(TextIO.<Point>writeCustomType()
//                //.to("E:\\Sweden\\KTH\\ID2221 Data Intensive Computing\\projectJava\\Project\\Project\\output.csv")
//                .to(new SerializableFunction<Point, DefaultFilenamePolicy.Params>() {
//                    @Override
//                    public DefaultFilenamePolicy.Params apply(Point input) {
//                        return new DefaultFilenamePolicy.Params().withBaseFilename("koutput");
//                    }
//                })
//        );

        pipeline.run().waitUntilFinish();

    }
//        double minDistance = Double.MAX_VALUE;
//        int closestCentroidId = -1;
//
//

//        PCollection<KV<Integer, Point>> newCentroids = points.apply(ParDo.of(new DoFn<Point, KV<Integer, Point>>() {
//            @ProcessElement
//            public void processElement(@Element Point point, OutputReceiver<KV<Integer, Point>> out1) {
//
//                PCollection< KV<Integer, Double>> nearestCentroid = centroids.apply(ParDo.of(new DoFn<Centroid, KV<Integer, Double>>() {
//                    @ProcessElement
//                    public void processElement(@Element Centroid centroid, OutputReceiver<KV<Integer, Double>> out) {
//                        // compute distance
//                        double distance = point.euclideanDistance(centroid);
//
//                        out.output(KV.of(Integer.valueOf(centroid.id), Double.valueOf(distance)));
//                    }
//                }
//                ));
//                nearestCentroid.apply(Min.perKey());
//
//                out1.output(KV.of(nearestCentroid, point));
//
//
//            })
//

//    }


    public static class KVComparator implements Comparator<KV<Integer, Double>>, Serializable {
        @Override
        public int compare(KV<Integer, Double> o1, KV<Integer, Double> o2) {
            return o1.getValue().compareTo(o2.getValue());
        }
    }


    private static PCollection<Point> getPointDataSet(PCollection<Data> data) {
        PCollection<Point> points = data.apply(ParDo.of(new DoFn<Data, Point>() {
            @ProcessElement
            public void processElement(@Element Data data, OutputReceiver<Point> out) {
                if (data.radio.equalsIgnoreCase("UMTS") || data.radio.equalsIgnoreCase("GSM")) {
                    out.output(new Point(data.lon, data.lat));
                }

            }
        }));

        return points;
    }

    private static PCollection<Centroid> getCentroidDataSet(PCollection<Data> data, int clusters) {
        PCollection<Centroid> centroids = data.apply(ParDo.of(new DoFn<Data, Centroid>() {
            @ProcessElement
            public void processElement(@Element Data data, OutputReceiver<Centroid> out) {
                if (data.radio.equalsIgnoreCase("LTE")) {
                    out.output(new Centroid(data.cell, data.lon, data.lat));
                }

            }
        })).apply(Sample.any(clusters));

        return centroids;
    }
}