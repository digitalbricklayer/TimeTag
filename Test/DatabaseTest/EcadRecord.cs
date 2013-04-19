namespace Openxtra.TimeTag.Database.Test
{
    using System;
    using FileHelpers;

    [ConditionalRecord(RecordCondition.ExcludeIfBegins, "#")]
    [IgnoreEmptyLines()]
    [IgnoreFirst(17)]
    [DelimitedRecord(",")]
    class EcadRecord
    {
// The fields are used by the FileHelpers library
#pragma warning disable 649

        // Station ID
        public long SOUID;

        [FieldConverter(ConverterKind.Date, "yyyyMMdd")]
        public DateTime DATE;

        [FieldConverter(typeof(EcadDataConverter))]
        public double RR;

        public int Q_RR;

        /// <summary>
        /// Precipitation is provided in multiples of 0.1mm increments. So, a 
        /// reading of 10 is really 10 * 0.1 = 1mm
        /// </summary>
	    private class EcadDataConverter : ConverterBase 
	    { 
            public override object StringToField(string from) 
	        {
	            double res = Convert.ToDouble(from); 
	            return res * 0.1; 
	        } 

	        public override string FieldToString(object from) 
	        { 
	            double d = (double) from; 
	            return Math.Round(d * 10).ToString(); 
	        } 
	    } 
    }
}
