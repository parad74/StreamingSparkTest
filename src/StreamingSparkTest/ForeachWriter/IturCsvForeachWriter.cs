using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;

namespace WebAPI.Model.AWS
{
    [Serializable]
    public class IturCsvForeachWriter : IForeachWriter
    {
        public List<Itur> IturList { get; set; }
       public IturCsvForeachWriter()
       {
           IturList =  new List<Itur>();
        }
        public virtual bool Open(long partitionId, long epochId)
        {
            try
            {
                //Console.WriteLine($"Open JosnForeachWriter ");
                this.IturList = new List<Itur>();
                return true;
            }
            catch
            {
                return false;
            }
        }
        public virtual void Process(Row value)
        {
           string iturCode = value.GetAs<string>("IturCode");
           string iturERP = value.GetAs<string>("IturERP");
           string quantityEdit = value.GetAs<string>("QuantityEdit");
           string partialQuantity = value.GetAs<string>("PartialQuantity");
   
            this.IturList.Add(new Itur { IturCode = iturCode, IturERP = iturERP, QuantityEdit = quantityEdit, PartialQuantity = partialQuantity });
        }
        public void Close(Exception errorOrNull)
        {
            if (errorOrNull != null)
            {
                // Console.WriteLine($"Error: {errorOrNull.Message}");
                return;
            }
            try
            {
                //Console.WriteLine($"Close JosnForeachWriter");
            }
            finally
            {

            }
        }
    }
}
