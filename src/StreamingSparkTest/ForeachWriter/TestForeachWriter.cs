using Microsoft.Spark.Sql;
using System;

namespace WebAPI.Model.AWS
{
    [Serializable]
    public class TestForeachWriter : IForeachWriter
    {
       public void Close(Exception errorOrNull)
        {
             if (errorOrNull != null)
            {
                Console.WriteLine($"Error: {errorOrNull.Message}");
                return;
            }
            try
            {
                Console.WriteLine($"Close TestForeachWriter");
            }
            finally
            {

            }
        }

        public bool Open(long partitionId, long epochId)
        {
            return true;
        }

         public void Process(Row value)
        {
           // Console.WriteLine($"==> {row.Get<string>("IturCode")} - {row.Get<string>("IturERP")} - {row.Get<string>("QuantityEdit")} - {row.Get<string>("PartialQuantity")}");
           //Console.WriteLine(value.Get(0));
           Console.WriteLine(" == yes ==");
       }
    }
}
