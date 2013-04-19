/*
 * Copyright 2008 OPENXTRA Limited
 * 
 * This file is part of TimeTag.
 * 
 * TimeTag is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * TimeTag is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with TimeTag.  If not, see <http://www.gnu.org/licenses/>.
 */

namespace Openxtra.TimeTag.Database
{
    using System;
    using System.Xml;
    using System.Text;
    using System.Diagnostics;
    using System.Collections.Generic;

    internal class XmlFileTimeSeriesDatabaseDao : ITimeSeriesDatabaseDao
    {
        private string filename;
        private XmlTextWriter writer;
        private XmlReader reader;

        // Archive templates indexed by sequence number
        private Dictionary<int, ArchiveTemplateDto> archiveTemplateIndex = new Dictionary<int, ArchiveTemplateDto>();

        public XmlFileTimeSeriesDatabaseDao(string filename)
        {
            this.filename = filename;
        }

        public void Connect(TimeSeriesDatabase.ConnectionMode mode)
        {
            // Not used by the XML DAO
        }

        public void Close()
        {
            // Not used by the XML DAO
        }

        public bool IsConnected()
        {
            return false;
        }

        public void Create(TimeSeriesDatabaseDto dto)
        {
            Debug.Assert(this.writer == null);

            using (this.writer = new XmlTextWriter(this.filename, Encoding.UTF8))
            {
                this.writer.WriteStartDocument();
                this.writer.WriteStartElement("TimeSeriesDatabase");
                WriteDatabaseAttributes(dto);
                WriteArchiveTemplates(dto.ArchiveTemplates);
                WriteDataSources(dto.DataSources);
                this.writer.WriteEndElement();
                this.writer.WriteEndDocument();

                this.writer.Close();
            }
        }

        public TimeSeriesDatabaseDto Read()
        {
            Debug.Assert(this.reader == null);

            XmlReaderSettings settings = new XmlReaderSettings();
            settings.IgnoreWhitespace = true;
            settings.IgnoreComments = true;

            TimeSeriesDatabaseDto dto = new TimeSeriesDatabaseDto();

            using (this.reader = XmlReader.Create(this.filename, settings))
            {
                this.reader.MoveToContent();
                this.reader.ReadStartElement("TimeSeriesDatabase");

                // Title
                this.reader.ReadStartElement("Title");
                dto.Title = this.reader.ReadString();
                this.reader.ReadEndElement();

                // Start time
                this.reader.ReadStartElement("StartTime");
                dto.StartTime = this.reader.ReadContentAsDateTime();
                this.reader.ReadEndElement();

                dto.ArchiveTemplates = ReadArchiveTemplates();
                dto.DataSources = ReadDataSources();

                this.reader.Close();
            }

            return dto;
        }

        public void Lock()
        {
            // Locking is not supported for XML
            throw new NotImplementedException();
        }

        public void Unlock()
        {
            throw new NotImplementedException();
        }

        public void SetTitle(string newTitle)
        {
            throw new NotImplementedException();
        }

        private DataSourceDto[] ReadDataSources()
        {
            List<DataSourceDto> dataSources = new List<DataSourceDto>();

            this.reader.Read();

            do
            {
                this.reader.Read();

                DataSourceDto dto = new DataSourceDto();

                // Name
                this.reader.ReadStartElement("Name");
                dto.Name = this.reader.ReadString();
                this.reader.ReadEndElement();

                // Type
                this.reader.ReadStartElement("Type");
                string typeAsString = this.reader.ReadString();
                dto.ConversionFunction = ConvertStringToConversionFunction(typeAsString);
                this.reader.ReadEndElement();

                // Interval
                this.reader.ReadStartElement("IntervalInSeconds");
                dto.PollingInterval = new TimeSpan(0, 0, this.reader.ReadContentAsInt());
                this.reader.ReadEndElement();

                // Min threshold
                this.reader.ReadStartElement("MinThreshold");
                double minThreshold = this.reader.ReadContentAsDouble();
                this.reader.ReadEndElement();

                // Max threshold
                this.reader.ReadStartElement("MaxThreshold");
                double maxThreshold = this.reader.ReadContentAsDouble();
                this.reader.ReadEndElement();

                dto.Range = new RangeDto();
                dto.Range.Min = minThreshold;
                dto.Range.Max = maxThreshold;

                // Last reading
                dto.LastReading = ReadLastReading();

                // Stats
                dto.Stats = ReadDataSourceStats();

                // Archives
                dto.Archives = ReadArchives();

                dataSources.Add(dto);
            } while (this.reader.ReadToNextSibling("DataSource"));

            this.reader.ReadEndElement();

            return dataSources.ToArray();
        }

        private ReadingDto ReadLastReading()
        {
            ReadingDto readingDto = null;

            if (this.reader.IsEmptyElement != true)
            {
                this.reader.Read();

                readingDto = new ReadingDto();

                // Value
                this.reader.ReadStartElement("Value");
                readingDto.Value = this.reader.ReadContentAsDouble();
                this.reader.ReadEndElement();

                // Timestamp
                this.reader.ReadStartElement("Timestamp");
                readingDto.Timestamp = this.reader.ReadContentAsDateTime();
                this.reader.ReadEndElement();

                this.reader.ReadEndElement();
            }
            else
            {
                this.reader.Read();
            }

            return readingDto;
        }

        private ArchiveTemplateDto[] ReadArchiveTemplates()
        {
            List<ArchiveTemplateDto> archiveTemplates = new List<ArchiveTemplateDto>();

            this.reader.Read();

            int sequenceNumber = 1;

            do
            {
                this.reader.Read();

                ArchiveTemplateDto dto = new ArchiveTemplateDto();

                // Name
                this.reader.ReadStartElement("Name");
                dto.Name = this.reader.ReadString();
                this.reader.ReadEndElement();

                // Type
                this.reader.ReadStartElement("Type");
                string typeAsString = this.reader.ReadString();
                dto.ConsolidationFunction = ConvertStringToConsolidationFunction(typeAsString);
                this.reader.ReadEndElement();

                // X factor
                this.reader.ReadStartElement("XFactor");
                dto.XFactor = this.reader.ReadContentAsInt();
                this.reader.ReadEndElement();

                // Readings per data point
                this.reader.ReadStartElement("ReadingsPerDataPoint");
                dto.ReadingsPerDataPoint = this.reader.ReadContentAsInt();
                this.reader.ReadEndElement();

                // Max data points
                this.reader.ReadStartElement("MaxDataPoints");
                dto.MaxDataPoints = this.reader.ReadContentAsInt();
                this.reader.ReadEndElement();

                archiveTemplates.Add(dto);
                this.archiveTemplateIndex.Add(sequenceNumber++, dto);
            } while (this.reader.ReadToNextSibling("ArchiveTemplate"));

            this.reader.ReadEndElement();

            return archiveTemplates.ToArray();
        }

        private ArchiveCollectionDto ReadArchives()
        {
            ArchiveCollectionDto archives = new ArchiveCollectionDto();

            int sequenceNumber = 1;

            do
            {
                this.reader.Read();

                ArchiveDto dto = new ArchiveDto(sequenceNumber);

                // Slot expiry timestamp
                this.reader.ReadStartElement("SlotExpiryTime");
                dto.SlotExpiryTime = this.reader.ReadContentAsDateTime();
                this.reader.ReadEndElement();

                // Accumulated readings
                dto.AccumulatedReadings = ReadAccumulatedReadings();

                // Data points
                dto.DataPointQueue = ReadDataPoints(sequenceNumber++);

                archives.Add(dto);
            } while (this.reader.ReadToNextSibling("Archive"));

            return archives;
        }

        private DataSourceStatsDto ReadDataSourceStats()
        {
            DataSourceStatsDto dto = new DataSourceStatsDto();

            this.reader.Read();

            // Total
            this.reader.ReadStartElement("Total");
            dto.Total = this.reader.ReadContentAsInt();
            this.reader.ReadEndElement();

            // Discarded
            this.reader.ReadStartElement("Discarded");
            dto.Discarded = this.reader.ReadContentAsInt();
            this.reader.ReadEndElement();

            this.reader.ReadEndElement();

            return dto;
        }

        private ReadingCollectionDto ReadAccumulatedReadings()
        {
            ReadingCollectionDto dto = new ReadingCollectionDto();

            if (this.reader.IsEmptyElement != true)
            {
                this.reader.Read();

                do
                {
                    this.reader.Read();

                    ReadingDto reading = new ReadingDto();

                    // Value
                    this.reader.ReadStartElement("Value");
                    reading.Value = this.reader.ReadContentAsDouble();
                    this.reader.ReadEndElement();

                    // Timestamp
                    this.reader.ReadStartElement("Timestamp");
                    reading.Timestamp = this.reader.ReadContentAsDateTime();
                    this.reader.ReadEndElement();

                    dto.Add(reading);
                } while (this.reader.ReadToNextSibling("Reading"));

                this.reader.ReadEndElement();
            }
            else
            {
                this.reader.Read();
            }

            return dto;
        }

        private DataPointCircularQueueDto ReadDataPoints(int sequenceNumber)
        {
            DataPointCircularQueueDto dto = new DataPointCircularQueueDto();

            Debug.Assert(this.archiveTemplateIndex.ContainsKey(sequenceNumber));

            dto.MaxDataPoints = this.archiveTemplateIndex[sequenceNumber].MaxDataPoints;

            if (this.reader.IsEmptyElement != true)
            {
                this.reader.Read();

                do
                {
                    this.reader.Read();

                    DataPointDto dataPoint = new DataPointDto();

                    // Value
                    this.reader.ReadStartElement("Value");
                    dataPoint.Value = this.reader.ReadContentAsDouble();
                    this.reader.ReadEndElement();

                    // Timestamp
                    this.reader.ReadStartElement("Timestamp");
                    dataPoint.Timestamp = this.reader.ReadContentAsDateTime();
                    this.reader.ReadEndElement();

                    dto.Add(dataPoint);
                } while (this.reader.ReadToNextSibling("DataPoint"));

                this.reader.ReadEndElement();
            }
            else
            {
                this.reader.Read();
            }

            return dto;
        }

        private void WriteDatabaseAttributes(TimeSeriesDatabaseDto dto)
        {
            // Title
            this.writer.WriteStartElement("Title");
            this.writer.WriteString(dto.Title);
            this.writer.WriteEndElement();
            // Start time
            this.writer.WriteStartElement("StartTime");
            this.writer.WriteValue(dto.StartTime);
            this.writer.WriteEndElement();
        }

        private void WriteDataSources(DataSourceDto[] dataSources)
        {
            this.writer.WriteStartElement("DataSources");
            foreach (DataSourceDto dto in dataSources)
            {
                WriteDataSource(dto);
            }
            this.writer.WriteEndElement();
        }

        private void WriteDataSource(DataSourceDto dto)
        {
            this.writer.WriteStartElement("DataSource");
                // Name
                this.writer.WriteStartElement("Name");
                this.writer.WriteString(dto.Name);
                this.writer.WriteEndElement();
                // Type
                this.writer.WriteStartElement("Type");
                this.writer.WriteValue(ConvertConversionFunctionToString(dto.ConversionFunction));
                this.writer.WriteEndElement();
                // Interval
                this.writer.WriteStartElement("IntervalInSeconds");
                this.writer.WriteValue(Convert.ToInt32(dto.PollingInterval.TotalSeconds));
                this.writer.WriteEndElement();
                // Min threshold
                this.writer.WriteStartElement("MinThreshold");
                this.writer.WriteValue(dto.Range.Min);
                this.writer.WriteEndElement();
                // Max threshold
                this.writer.WriteStartElement("MaxThreshold");
                this.writer.WriteValue(dto.Range.Max);
                this.writer.WriteEndElement();
                WriteLastReading(dto.LastReading);
                WriteStats(dto.Stats);
                WriteArchives(dto.Archives);
            this.writer.WriteEndElement();
        }

        private void WriteLastReading(ReadingDto readingDto)
        {
            // Last reading
            this.writer.WriteStartElement("LastReading");

            if (readingDto != null)
            {
                // Value
                this.writer.WriteStartElement("Value");
                this.writer.WriteValue(readingDto.Value);
                this.writer.WriteEndElement();
                // Timestamp
                this.writer.WriteStartElement("Timestamp");
                this.writer.WriteValue(readingDto.Timestamp);
                this.writer.WriteEndElement();
            }
            this.writer.WriteEndElement();
        }

        private void WriteArchiveTemplates(ArchiveTemplateDto[] archiveTemplates)
        {
            this.writer.WriteStartElement("ArchiveTemplates");
            foreach (ArchiveTemplateDto dto in archiveTemplates)
            {
                WriteArchiveTemplate(dto);
            }
            this.writer.WriteEndElement();
        }

        private void WriteArchiveTemplate(ArchiveTemplateDto dto)
        {
            this.writer.WriteStartElement("ArchiveTemplate");
                // Name
                this.writer.WriteStartElement("Name");
                this.writer.WriteString(dto.Name);
                this.writer.WriteEndElement();
                // Type
                this.writer.WriteStartElement("Type");
                this.writer.WriteValue(ConvertConsolidationFunctionToString(dto.ConsolidationFunction));
                this.writer.WriteEndElement();
                // X factor
                this.writer.WriteStartElement("XFactor");
                this.writer.WriteValue(dto.XFactor);
                this.writer.WriteEndElement();
                // Readings per data point
                this.writer.WriteStartElement("ReadingsPerDataPoint");
                this.writer.WriteValue(dto.ReadingsPerDataPoint);
                this.writer.WriteEndElement();
                // Max data points
                this.writer.WriteStartElement("MaxDataPoints");
                this.writer.WriteValue(dto.MaxDataPoints);
                this.writer.WriteEndElement();
            this.writer.WriteEndElement();
        }

        private void WriteArchives(ArchiveCollectionDto archives)
        {
            foreach (ArchiveDto dto in archives)
            {
                WriteArchive(dto);
            }
        }

        private void WriteArchive(ArchiveDto dto)
        {
            this.writer.WriteStartElement("Archive");
                // Slot expiry timestamp
                this.writer.WriteStartElement("SlotExpiryTime");
                this.writer.WriteValue(dto.SlotExpiryTime);
                this.writer.WriteEndElement();
                WriteAccumulatedReadings(dto.AccumulatedReadings);
                WriteDataPoints(dto.DataPointQueue);
            this.writer.WriteEndElement();
        }

        private void WriteStats(DataSourceStatsDto dto)
        {
            this.writer.WriteStartElement("Stats");
                // Total
                this.writer.WriteStartElement("Total");
                this.writer.WriteValue(dto.Total);
                this.writer.WriteEndElement();
                // Discarded
                this.writer.WriteStartElement("Discarded");
                this.writer.WriteValue(dto.Discarded);
                this.writer.WriteEndElement();
            this.writer.WriteEndElement();
        }

        private void WriteAccumulatedReadings(ReadingCollectionDto dto)
        {
            this.writer.WriteStartElement("AccumulatedReadings");
            ReadingDto[] readings = dto.Readings;
            foreach (ReadingDto reading in readings)
            {
                this.writer.WriteStartElement("Reading");

                // Value
                this.writer.WriteStartElement("Value");
                this.writer.WriteValue(reading.Value);
                this.writer.WriteEndElement();
                // Timestamp
                this.writer.WriteStartElement("Timestamp");
                this.writer.WriteValue(reading.Timestamp);
                this.writer.WriteEndElement();

                this.writer.WriteEndElement();
            }
            this.writer.WriteEndElement();
        }

        private void WriteDataPoints(DataPointCircularQueueDto dto)
        {
            this.writer.WriteStartElement("DataPoints");
            DataPointDto[] dataPoints = dto.DataPoints;
            foreach (DataPointDto dataPoint in dataPoints)
            {
                this.writer.WriteStartElement("DataPoint");

                // Value
                this.writer.WriteStartElement("Value");
                this.writer.WriteValue(dataPoint.Value);
                this.writer.WriteEndElement();
                // Timestamp
                this.writer.WriteStartElement("Timestamp");
                this.writer.WriteValue(dataPoint.Timestamp);
                this.writer.WriteEndElement();

                this.writer.WriteEndElement();
            }
            this.writer.WriteEndElement();
        }

        private string ConvertConversionFunctionToString(int conversionFunction)
        {
            string conversionFunctionAsString;
            switch (conversionFunction)
            {
                case 0:
                    conversionFunctionAsString = "GAUGE";
                    break;

                case 1:
                    conversionFunctionAsString = "ABSOLUTE";
                    break;

                case 2:
                    conversionFunctionAsString = "DERIVE";
                    break;

                case 3:
                    conversionFunctionAsString = "COUNTER";
                    break;

                default:
                    throw new ArgumentException("Invalid conversion function");
            }

            return conversionFunctionAsString;
        }

        private int ConvertStringToConversionFunction(string conversionFunctionAsString)
        {
            int conversionFunction;
            switch (conversionFunctionAsString)
            {
                case "GAUGE":
                    conversionFunction = 0;
                    break;

                case "ABSOLUTE":
                    conversionFunction = 1;
                    break;

                case "DERIVE":
                    conversionFunction = 2;
                    break;

                case "COUNTER":
                    conversionFunction = 3;
                    break;

                default:
                    throw new ArgumentException("Invalid conversion function");
            }

            return conversionFunction;
        }

        private string ConvertConsolidationFunctionToString(int consolidationFunction)
        {
            string consolidationFunctionAsString;
            switch (consolidationFunction)
            {
                case 0:
                    consolidationFunctionAsString = "MIN";
                    break;

                case 1:
                    consolidationFunctionAsString = "MAX";
                    break;

                case 2:
                    consolidationFunctionAsString = "LAST";
                    break;

                case 3:
                    consolidationFunctionAsString = "AVERAGE";
                    break;

                default:
                    throw new ArgumentException("Invalid consolidation function");
            }

            return consolidationFunctionAsString;
        }

        private int ConvertStringToConsolidationFunction(string consolidationFunctionAsString)
        {
            int consolidationFunction;

            switch (consolidationFunctionAsString)
            {
                case "MIN":
                    consolidationFunction = 0;
                    break;

                case "MAX":
                    consolidationFunction = 1;
                    break;

                case "LAST":
                    consolidationFunction = 2;
                    break;

                case "AVERAGE":
                    consolidationFunction = 3;
                    break;

                default:
                    throw new ArgumentException("Invalid consolidation function");
            }

            return consolidationFunction;
        }
    }
}
