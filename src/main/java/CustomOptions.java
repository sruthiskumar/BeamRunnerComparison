import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface CustomOptions extends PipelineOptions {

    @Description("The number of iterations")
    @Default.Integer(10)
    int getIterations();
    void setIterations(int iterations);

    @Description("The number of clusters")
    @Default.Integer(500)
    int getclusters();
    void setClusters(int clusters);
}
