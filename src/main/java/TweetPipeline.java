import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class TweetPipeline {
    public static void main(String... args){

        //PipelineOptionsFactory.register(CustomOptions.class);
        PipelineOptions options = PipelineOptionsFactory.create(); //fromArgs(args).as(CustomOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        //read tweets from JSON file
        PCollection<String> lines = pipeline.apply(TextIO.read().from("diwali_tweets.jsonl"));

        //parse the json
        PCollection<Tweet> tweets =  lines.apply(ParseJsons.of(Tweet.class))
                .setCoder(SerializableCoder.of(Tweet.class));

        //extract user names
        PCollection<String> users =  tweets.apply(MapElements
                .into(TypeDescriptors.strings())
                .via(t -> t.user.screenName)
        );

        //filter to get the users with tweets > 5
        PCollection<KV<String, Long>> frequentUsers =
                users.apply(Count.perElement())
                .apply(Filter.by(kv -> kv.getValue()>=5));




        //write the output to a file
        frequentUsers.apply(MapElements.into(TypeDescriptors.strings()).via(Object::toString))
                .apply(TextIO.write().to("tweet_count"));
        //execute
        pipeline.run().waitUntilFinish();
    }
}
