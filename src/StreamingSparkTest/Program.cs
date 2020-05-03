using System;
using System.IO;
using System.Reflection;
using System.Threading;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Streaming;
using Microsoft.Spark.Sql.Types;

namespace WebAPI.Model.AWS
{
    class Program
    {
        //spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --master local C:\Count4U\S3\SparkTest\StreamingSparkTest\src\StreamingSparkTest\bin\Debug\netcoreapp3.1\microsoft-spark-2.4.x-0.10.0.jar debug

        private static readonly AutoResetEvent s_waitHandle = new AutoResetEvent(false);
        private static StreamingQuery s_query;
        static void Main(string[] args)
        {
            SetupExitHandlers();
            Console.WriteLine("Press CTRL+C to exit.");
            Console.WriteLine();
            string assemblyLocation = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) ;
            string dataPath = Path.Combine(assemblyLocation, "data");
            RunSparkStream(dataPath);
                                                                                  // Wait for signal on the main thread
            s_waitHandle.WaitOne();
        }
        private static void SetupExitHandlers()
        {
            Console.CancelKeyPress += (s, e) =>
            {
                Console.WriteLine($"{Environment.NewLine}Ctrl+C pressed");
                Environment.Exit(0);
            };
            AppDomain.CurrentDomain.ProcessExit += (s, e) =>
            {
                Console.WriteLine($"{Environment.NewLine}Exiting");

            // stop spark query
            s_query.Stop();
            // Allow the main thread to continue and exit...
            s_waitHandle.Set();
            };
        }
        public static void RunSparkStream(string streamInputPath)
        {
            var foreachWriter = new TestForeachWriter();

            SparkSession spark = SparkSession
                    .Builder()
                    .AppName("itur")
                    .GetOrCreate();


            var mySchema = new Microsoft.Spark.Sql.Types.StructType(new[]
                 {
                    new StructField("IturCode", new Microsoft.Spark.Sql.Types.StringType()),
                    new StructField("IturERP", new Microsoft.Spark.Sql.Types.StringType()) ,
                    new StructField("QuantityEdit", new Microsoft.Spark.Sql.Types.StringType()),
                    new StructField("PartialQuantity", new Microsoft.Spark.Sql.Types.StringType())
                });

            DataFrame lines = spark
                    .ReadStream()
                   .Schema(mySchema)
                  .Csv(streamInputPath);

            s_query = lines
                 .WriteStream()
                  .Foreach(foreachWriter)
                 .Trigger(Trigger.ProcessingTime(5000))
                 .Start();


            s_query.AwaitTermination();
        }
    }
}