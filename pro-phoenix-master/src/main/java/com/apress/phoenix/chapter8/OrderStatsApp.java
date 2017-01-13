package com.apress.phoenix.chapter8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.mapreduce.PhoenixOutputFormat;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * map reduce job to compute order aggregates
 */
public class OrderStatsApp extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(OrderStatsApp.class);

    @Override
    public int run(String[] args) throws Exception {
        try {
            final Configuration configuration = HBaseConfiguration.create(getConf());
            setConf(configuration);
            final Job job = Job.getInstance(configuration, "phoenix-mr-order_stats-job");
            final String selectQuery = "SELECT ORDER_ID, CUST_ID, AMOUNT FROM ORDERS ";
            // set the input table and select query. you can also pass in the list of columns
            PhoenixMapReduceUtil.setInput(job, OrderWritable.class, "ORDERS", selectQuery);
            // set the output table name and the list of columns.
            PhoenixMapReduceUtil.setOutput(job, "ORDER_STATS", "CUST_ID, AMOUNT");
            job.setMapperClass(OrderMapper.class);
            job.setReducerClass(OrderReducer.class);
            job.setOutputFormatClass(PhoenixOutputFormat.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(OrderWritable.class);
            TableMapReduceUtil.addDependencyJars(job);
            job.waitForCompletion(true);
            return 0;
        } catch (Exception ex) {
            LOG.error(String.format("An exception [%s] occurred while performing the job: ", ex.getMessage()));
            return -1;
        }
    }

    public static void main(String[] args) throws Exception{
        int status = ToolRunner.run( new OrderStatsApp(), args);
        System.exit(status);
    }

    public static class OrderMapper extends Mapper<NullWritable, OrderWritable, LongWritable, DoubleWritable> {
        private LongWritable customerId = new LongWritable();
        private DoubleWritable amount = new DoubleWritable();

        @Override
        protected void map(NullWritable key, OrderWritable order, Context context)
                throws IOException, InterruptedException {
            // leaving out data validation for brevity.
            customerId.set(order.customerId);
            amount.set(order.amount);
            context.write(customerId, amount);
        }
    }

    public static class OrderReducer extends Reducer<LongWritable, DoubleWritable, NullWritable, OrderWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<DoubleWritable> amounts, Context context)
                throws IOException, InterruptedException {
            // keeping only the core logic here.
            double totalValue = 0.0;
            for(DoubleWritable amount : amounts) {
                totalValue += amount.get();
            }
            context.write(NullWritable.get(), new OrderWritable(key.get(), totalValue));
        }
    }

    public static class OrderWritable implements DBWritable, Writable {
        private Long customerId;
        private Double amount;

        public OrderWritable() { }

        public OrderWritable(Long customerId, Double amount) {
            this.customerId = customerId;
            this.amount = amount;
        }

        @Override
        public void write(PreparedStatement preparedStatement) throws SQLException {
            preparedStatement.setLong(1, customerId);
            preparedStatement.setDouble(2, amount);
        }

        @Override
        public void readFields(ResultSet resultSet) throws SQLException {
            customerId = resultSet.getLong("CUST_ID");
            amount = resultSet.getDouble("AMOUNT");
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeLong(customerId);
            dataOutput.writeDouble(amount);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.customerId = dataInput.readLong();
            this.amount = dataInput.readDouble();
        }
    }
}