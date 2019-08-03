import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
public class ReadWriteSparkS3 {
    public static List<String> getStaticPath() {
        String base = "input/2019-07-10/";
        List<String> listOfFolders = new ArrayList<String>();
        listOfFolders.add(base + "00");
        for (int i = 1; i < 4; i++) {
            listOfFolders.add(base + String.format("%02d", i));
        }
        return listOfFolders;
    }
    public static void main(String[] args) {
        List<String> listOfFolders =getStaticPath();
        SparkConf conf = new SparkConf().setMaster("local").setAppName("test").set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2").set("spark.speculation", "false");;
        @SuppressWarnings("resource")
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> javaRdd2 = jsc.parallelize(listOfFolders, listOfFolders.size())
                .mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
                    private static final long serialVersionUID = 54545;

                    public Iterator<String> call(Iterator<String> t) throws Exception {
                        List<String> list = new ArrayList<String>();
                        BasicAWSCredentials awsCreds = new BasicAWSCredentials("accessKey", "secretKey");
                        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withRegion(Regions.AP_SOUTH_1).withForceGlobalBucketAccessEnabled(true).build();
                        ListObjectsRequest request = new ListObjectsRequest();
                        request.setBucketName("newbucketadarshnow");
                        List<String> objectList = new ArrayList<String>();
                        while (t.hasNext()) {
                            request.setPrefix(t.next());
                            ObjectListing objectLising = s3.listObjects(request);
                            List<S3ObjectSummary> lists = objectLising.getObjectSummaries();
                            for (S3ObjectSummary key : lists) {
                                objectList.add(key.getKey());
                            }
                        }
                        list.addAll(objectList);
                        return list.iterator();
                    }
                }).mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
                    private static final long serialVersionUID = 1232323;

                    public Iterator<String> call(Iterator<String> t) throws Exception {
                        List<String> list = new ArrayList<String>();
                        BasicAWSCredentials awsCreds = new BasicAWSCredentials("accessKey", "secretKey");
                        final AmazonS3 s33 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withRegion(Regions.AP_SOUTH_1).withForceGlobalBucketAccessEnabled(true).build();
                        while (t.hasNext()) {
                            String fileName = t.next();
                            if (!fileName.endsWith("/")) {
                                StringWriter writer = new StringWriter();
                                IOUtils.copy(s33.getObject("newbucketadarshnow", fileName).getObjectContent(), writer);
                                list.addAll(Arrays.asList(writer.toString().split("\n")));
                            }
                        }
                        return list.iterator();
                    }
                });
        javaRdd2.saveAsTextFile("s3a://accessKey:secretKey@newbucketadarshnow/output");
    }
}