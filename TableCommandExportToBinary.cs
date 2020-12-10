using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Reflection;
using System.Text;

namespace DataMover
{
	public static partial class DataMover
	{
		private class TableCommandExportToBinary : TableCommandExport
		{
			protected override string DataFileType => "dat";

			public TableCommandExportToBinary()
			{
			}

			public TableCommandExportToBinary(string databaseName, string tableName, string dataFileName, string path) : base(databaseName, tableName, dataFileName, path)
			{
			}

			protected override void GenDynamicMethod(StringBuilder sb)
			{
				sb.AppendLine(@"public static void DynamicMethod(SqlDataReader reader, BinaryWriter bw)");
				sb.AppendLine("{");

				TableColumns.ForEach(c => c.GenWriteBinary(sb));

				sb.AppendLine("}");
			}

			private delegate void DynamicMethodDelegate(SqlDataReader reader, BinaryWriter bw);

			public override int ExecInner(MethodInfo methodInfo)
			{
				TraceLog.WriteConsole($"Exporting table {FullyQualifiedTableName}");

				var WriteRowDynamic = (DynamicMethodDelegate)methodInfo.CreateDelegate(typeof(DynamicMethodDelegate));

				var recIdx = 0;

				using (var fileStream = new FileStream(DataFileName, FileMode.Create))
				using (var gZipStream = new GZipStream(fileStream, CompressionMode.Compress))
				using (var bufferedStream = new BufferedStream(gZipStream, BufferSize))
				using (var bw = new BinaryWriter(bufferedStream))
				using (var reader = SourceSqlCommand.ExecuteReader())
				{
					WriteBinaryHeader(bw);

					while (reader.Read())
					{
						bw.Write(recIdx++);

						WriteRowDynamic(reader, bw);

						if (recIdx % LogRecordCountEvery == 0)
						{
							TraceLog.WriteConsole($"Records: {recIdx}");
						}
					}

					bw.Write(EofIndex);
				}

				return recIdx;
			}
		}
	}
}