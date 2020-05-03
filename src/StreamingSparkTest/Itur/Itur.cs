using System;

namespace WebAPI.Model.AWS
{
	[Serializable]
	public class Itur
	{
		public string IturCode { get; set;}
		public string IturERP { get; set;}
		public string QuantityEdit { get; set;}
		public string PartialQuantity { get; set;}
	}
}
