using System.Data.SqlClient;
using System.Reflection;
using System.Text;

namespace DataMover
{
	public static partial class DataMover
	{
		private class TableCommandExportToHtml : TableCommandExport
		{
			protected override string DataFileType => "html";

			public TableCommandExportToHtml()
			{
			}

			public TableCommandExportToHtml(string databaseName, string tableName, string dataFileName, string path) : base(databaseName, tableName, dataFileName, path)
			{
			}

			protected override void GenDynamicMethod(StringBuilder sb)
			{
				sb.AppendLine(@"public static void DynamicMethod(SqlDataReader reader)");
				sb.AppendLine("{");

				TableColumns.ForEach(c => c.GenWriteHtml(sb));

				sb.AppendLine("}");
			}

			private delegate void DynamicMethodDelegate(SqlDataReader reader);

			public override int ExecInner(MethodInfo methodInfo)
			{
				TraceLog.WriteConsole($"Exporting table {FullyQualifiedTableName} to Html");

				var WriteRowDynamic = (DynamicMethodDelegate)methodInfo.CreateDelegate(typeof(DynamicMethodDelegate));

				var recIdx = 0;

				SaveExistingFile(DataFileName);

				Html.Start();
				Html.Section();
				Html.Table();

				using (var reader = SourceSqlCommand.ExecuteReader())
				{
					//TODO
					//WriteFileHeader(bw);

					while (reader.Read())
					{
						Html.Row();

						WriteRowDynamic(reader);

						Html.EndRow();

						if (recIdx % LogRecordCountEvery == 0)
						{
							TraceLog.WriteConsole($"Records: {recIdx}");
						}

						recIdx++;
					}

					Html.EndTable();
					Html.EndSection();

					Html.Stop(DataFileName);
				}

				return recIdx;
			}
		}
	}
}