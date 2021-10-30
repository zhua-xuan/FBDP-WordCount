import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class WordCount {
    public static class TokenizerFileMapper
            extends Mapper<Object, Text, Text, IntWritable>{
                static enum CountersEnum { INPUT_WORDS }

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> wordsToSkip = new HashSet<String>();
        private Set<String> punctuations = new HashSet<String>();
        private Configuration conf;
        private BufferedReader fis;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();

            if (conf.getBoolean("wordcountskip", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                Path patternsPath = new Path(patternsURIs[0].getPath());
                String patternsFileName = patternsPath.getName().toString();
                parseSkipFile(patternsFileName);
                Path punctuationsPath = new Path(patternsURIs[1].getPath());
                String punctuationsFileName = punctuationsPath.getName().toString();
                parseSkipPunctuations(punctuationsFileName);
            }
        }

        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    wordsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file "
                        + StringUtils.stringifyException(ioe));
            }
        }


        private void parseSkipPunctuations(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    punctuations.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file "
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String textName = fileSplit.getPath().getName();

            String line = value.toString().toLowerCase();
            for (String pattern : punctuations) {
                line = line.replaceAll(pattern, " ");
            }
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                String one_word = itr.nextToken();
                if(wordsToSkip.contains(one_word)){
                    continue;
                }
                word.set(one_word+"#"+textName);
                context.write(word, one);
            }
        }
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
        static enum CountersEnum { INPUT_WORDS }
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> patternsToSkip = new HashSet<String>();
        private Set<String> punctuations = new HashSet<String>();
        private Configuration conf;
        private BufferedReader fis;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();

            if (conf.getBoolean("wordcountskip", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();

                Path patternsPath = new Path(patternsURIs[0].getPath());
                String patternsFileName = patternsPath.getName().toString();

                Path punctuationsPath = new Path(patternsURIs[1].getPath());
                String punctuationsFileName = punctuationsPath.getName().toString();
                parseSkipPunctuations(punctuationsFileName);
            }
        }
        private void parseSkipPunctuations(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    punctuations.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file "
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();
            for (String pattern : punctuations) {
                line = line.replaceAll(pattern, " ");
            }
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                String one_word = itr.nextToken();
//
                if(one_word.length()<3) {
                    continue;
                }
                if(Pattern.compile("^[-\\+]?[\\d]*$").matcher(one_word).matches()) {
                    continue;
                }
                if(patternsToSkip.contains(one_word)){
                    continue;
                }

                word.set(one_word);
                context.write(word, one);

                Counter counter = context.getCounter(
                        CountersEnum.class.getName(),
                        CountersEnum.INPUT_WORDS.toString());
                counter.increment(1);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class SortFileReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
        private MultipleOutputs<Text,NullWritable> mos;
        protected void setup(Context context) throws IOException,InterruptedException {
            mos = new MultipleOutputs<Text, NullWritable>(context);
        }
        protected void cleanup(Context context) throws IOException,InterruptedException {
            mos.close();
        }

        private Text result = new Text();
        private HashMap<String, Integer> map = new HashMap<>();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            for(Text val: values){
                String txt = val.toString().split("#")[1];
                txt = txt.substring(0, txt.length()-4);
                txt = txt.replaceAll("-", "");
                String oneWord = val.toString().split("#")[0];
                int sum = map.values().stream().mapToInt(i->i).sum();
                int count = map.getOrDefault(txt, 0);
                if(count == 100){
                    continue;
                }
                else {
                    count += 1;
                    map.put(txt, count); //0->1, n->n+1
                }
                result.set(oneWord.toString());
                String str=count+": "+result+", "+key;
                mos.write(txt, new Text(str), NullWritable.get() );
            }
        }
    }

    public static class SortAllReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
        private Text result = new Text();
        int rank=1;

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            for(Text val: values){
                if(rank > 100)
                {
                    break;
                }
                result.set(val.toString());
                String str=rank+": "+result+", "+key;
                rank++;
                context.write(new Text(str),NullWritable.get());
            }
        }
    }
    public static void main(String[] args) throws Exception, URISyntaxException {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 2) && (remainingArgs.length != 5)) {
            System.err.println("Usage: wordcount <in> <out> [-skip punctuations skipPatternFile]");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "wordcount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerFileMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]+"/tmp"));
        List<String> otherArgs = new ArrayList<String>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("wordcountskip", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        Path tempDir = new Path("tmp" );
        FileOutputFormat.setOutputPath(job, tempDir);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        if(job.waitForCompletion(true))
        {
            Job sortJob = Job.getInstance(conf, "sortfile");
            sortJob.setJarByClass(WordCount.class);

            FileInputFormat.addInputPath(sortJob, tempDir);
            sortJob.setInputFormatClass(SequenceFileInputFormat.class);
            sortJob.setMapperClass(InverseMapper.class);
            sortJob.setReducerClass(SortFileReducer.class);

            Path tempFileDir = new Path("results" );
            FileOutputFormat.setOutputPath(sortJob, tempFileDir);

            List<String> fileNameList = Arrays.asList("shakespearealls11", "shakespeareantony23",
                    "shakespeareas12", "shakespearecomedy7", "shakespearecoriolanus24",
                    "shakespearecymbeline17", "shakespearefirst51",
                    "shakespearehamlet25", "shakespearejulius26",
                    "shakespeareking45", "shakespearelife54",
                    "shakespearelife55", "shakespearelife56",
                    "shakespearelovers62", "shakespeareloves8",
                    "shakespearemacbeth46", "shakespearemeasure13",
                    "shakespearemerchant5", "shakespearemerry15",
                    "shakespearemidsummer16", "shakespearemuch3",
                    "shakespeareothello47", "shakespearepericles21",
                    "shakespearerape61", "shakespeareromeo48",
                    "shakespearesecond52", "shakespearesonnets59",
                    "shakespearesonnets", "shakespearetaming2",
                    "shakespearetempest4", "shakespearethird53",
                    "shakespearetimon49", "shakespearetitus50",
                    "shakespearetragedy57", "shakespearetragedy58",
                    "shakespearetroilus22", "shakespearetwelfth20",
                    "shakespearetwo18", "shakespearevenus60",
                    "shakespearewinters19");

            for (String fileName : fileNameList) {
                MultipleOutputs.addNamedOutput(sortJob, fileName, TextOutputFormat.class,Text.class, NullWritable.class);
            }
            sortJob.setOutputKeyClass(IntWritable.class);
            sortJob.setOutputValueClass(Text.class);
            sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
            if(sortJob.waitForCompletion(true)){
                Job job1 = Job.getInstance(conf, "allwordcount");
                job1.setJarByClass(WordCount.class);
                job1.setMapperClass(TokenizerMapper.class);
                job1.setCombinerClass(IntSumReducer.class);
                job1.setReducerClass(IntSumReducer.class);
                job1.setOutputKeyClass(Text.class);
                job1.setOutputValueClass(IntWritable.class);
                for (int i = 0; i < remainingArgs.length; ++i) {
                    if ("-skip".equals(remainingArgs[i])) {
                        job1.addCacheFile(new Path(remainingArgs[++i]).toUri());
                        job1.addCacheFile(new Path(remainingArgs[++i]).toUri());
                        job1.getConfiguration().setBoolean("wordcountskip", true);
                    } else {
                        otherArgs.add(remainingArgs[i]);
                    }
                }

                FileInputFormat.addInputPath(job1, new Path(otherArgs.get(0)));
                Path tempAllDir = new Path("tmp" );
                FileOutputFormat.setOutputPath(job1, tempAllDir);
                job1.setOutputFormatClass(SequenceFileOutputFormat.class);

                if(job1.waitForCompletion(true)){
                    Job sortJob1 = Job.getInstance(conf, "sortall");
                    sortJob1.setJarByClass(WordCount.class);
                    FileInputFormat.addInputPath(sortJob1, tempAllDir);
                    sortJob1.setInputFormatClass(SequenceFileInputFormat.class);
                    sortJob1.setMapperClass(InverseMapper.class);
                    sortJob1.setReducerClass(SortAllReducer.class);
                    FileOutputFormat.setOutputPath(sortJob1, new Path(otherArgs.get(1)));
                    sortJob1.setOutputKeyClass(IntWritable.class);
                    sortJob1.setOutputValueClass(Text.class);
                    sortJob1.setSortComparatorClass(IntWritableDecreasingComparator.class);
                    System.exit(sortJob1.waitForCompletion(true) ? 0 : 1);
                }
                boolean b = job1.waitForCompletion(true);
                Path temppath= new Path(args[1]+"/tmp");
                temppath.getFileSystem(conf).deleteOnExit(temppath);
                if(!b){
                    System.out.println("sort task fail!");
                }
            }
        }
    }

}