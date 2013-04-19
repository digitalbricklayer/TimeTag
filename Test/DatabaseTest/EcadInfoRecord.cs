namespace Openxtra.TimeTag.Database.Test
{
    using System;
    using FileHelpers;

    [IgnoreEmptyLines()]
    [IgnoreFirst(25)]
    [DelimitedRecord("|")]
    class EcadInfoRecord
    {
// The fields are used by the FileHelpers library
#pragma warning disable 649

        [FieldTrim(TrimMode.Both)]
        public string COUNTRY;
        
        [FieldTrim(TrimMode.Both)]
        public string SOUNAME;
        
        public long SOUID;

        [FieldTrim(TrimMode.Both)]
        public string LAT;

        [FieldTrim(TrimMode.Both)]
        public string LON;

        [FieldTrim(TrimMode.Both)]
        public string HGHT;

        [FieldTrim(TrimMode.Both)]
        public string ELE;

        [FieldConverter(ConverterKind.Date, "yyyyMMdd")]
        public DateTime START;

        [FieldConverter(ConverterKind.Date, "yyyyMMdd")]
        public DateTime STOP;

        [FieldTrim(TrimMode.Both)]
        public string P;

        [FieldTrim(TrimMode.Both)]
        public string PARID;

        [FieldTrim(TrimMode.Both)]
        public string PARNAME;

#pragma warning restore 649
    }
}
