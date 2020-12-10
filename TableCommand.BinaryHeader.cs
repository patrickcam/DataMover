using System.Data;
using System.IO;
using System.IO.Compression;

namespace DataMover
{
	public static partial class DataMover
	{
		internal partial class TableCommand
		{
			protected void ReadBinaryHeader()
			{
				using (var fileStream = new FileStream(DataFileName, FileMode.Open))
				using (var gZipStream = new GZipStream(fileStream, CompressionMode.Decompress))
				using (var bufferedStream = new BufferedStream(gZipStream, BufferSize))
				using (var br = new BinaryReader(bufferedStream))
				{
					var fileCheckMark = br.ReadInt64();
					if (fileCheckMark != FileCheckMark)
					{
						throw new TraceLog.InternalException("The input file is not a valid export file");
					}

					var checkTableName = br.ReadString();

					var colCnt = br.ReadInt32();

					for (var cnt = 1; cnt <= colCnt; cnt++)
					{
						var newCol = Column.ReadFromHeader(br);
						SourceColumns.Add(newCol);
					}

					fileCheckMark = br.ReadInt64();
					if (fileCheckMark != FileCheckMark)
					{
						throw new TraceLog.InternalException("Missing end of header mark");
					}
				}
			}

			protected static void SkipBinaryHeader(BinaryReader br)
			{
				{
					var fileCheckMark = br.ReadInt64();
					if (fileCheckMark != FileCheckMark)
					{
						throw new TraceLog.InternalException("The input file is not a valid export file");
					}

					var checkTableName = br.ReadString();

					var colCnt = br.ReadInt32();

					for (var cnt = 1; cnt <= colCnt; cnt++)
					{
						Column.ReadFromHeader(br);
					}

					fileCheckMark = br.ReadInt64();
					if (fileCheckMark != FileCheckMark)
					{
						throw new TraceLog.InternalException("Missing end of header mark");
					}
				}
			}

			public void WriteBinaryHeader(BinaryWriter bw)
			{
				bw.Write(FileCheckMark);
				bw.Write(TableName);
				bw.Write(TableColumns.Count);

				TableColumns.ForEach(c => c.WriteToHeader(bw));

				bw.Write(FileCheckMark);
			}

			public void CheckBinaryHeader(BinaryReader br, DataTable datatable)
			{
				var fileCheck = br.ReadInt64();
				if (fileCheck != FileCheckMark)
				{
					throw new TraceLog.InternalException("The input file is not a valid export file");
				}

				var checkTableName = br.ReadString();
				var colCnt = br.ReadInt32();

				foreach (DataColumn col in datatable.Columns)
				{
					var colName = br.ReadString();

					if (colName != col.ColumnName)
					{
						throw new TraceLog.InternalException($"Column name mismatch, tab:{col.ColumnName}, file:{colName}");
					}
				}

				if (colCnt != datatable.Columns.Count)
				{
					TraceLog.WriteConsole("Columns in table:");
					foreach (DataColumn col in datatable.Columns)
					{
						TraceLog.WriteConsole(col.ColumnName);
					}

					TraceLog.WriteConsole("Columns in file:");
					for (var idx = 0; idx < colCnt; idx++)
					{
						TraceLog.WriteConsole(br.ReadString());
					}

					throw new TraceLog.InternalException($"Column count mismatch, tab:{datatable.Columns.Count}, file:{colCnt}");
				}

				fileCheck = br.ReadInt64();
				if (fileCheck != FileCheckMark)
				{
					throw new TraceLog.InternalException("Missing end of header mark");
				}
			}
		}
	}
}