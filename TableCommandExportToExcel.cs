using OfficeOpenXml;
using System.Data.SqlClient;
using System.IO;
using System.Reflection;
using System.Text;

namespace DataMover
{
	public static partial class DataMover
	{
		private partial class TableCommandExportToExcel : TableCommandExport
		{
			protected override string DataFileType => "xlsx";

			public TableCommandExportToExcel()
			{
			}

			public TableCommandExportToExcel(string databaseName, string tableName, string dataFileName, string path) : base(databaseName, tableName, dataFileName, path)
			{
			}

			protected override void GenDynamicMethod(StringBuilder sb)
			{
				sb.AppendLine(@"public static void DynamicMethod(SqlDataReader reader, ExcelWorksheet ws, int recIdx)");
				sb.AppendLine("{");

				TableColumns.ForEach(c => c.GenWriteExcel(sb));

				sb.AppendLine("}");
			}

			private delegate void DynamicMethodDelegate(SqlDataReader reader, ExcelWorksheet ws, int recIdx);

			public override int ExecInner(MethodInfo methodInfo)
			{
				TraceLog.WriteConsole($"Exporting table {FullyQualifiedTableName} to Excel");

				var WriteRowDynamic = (DynamicMethodDelegate)methodInfo.CreateDelegate(typeof(DynamicMethodDelegate));

				var recCnt = 0;

				SaveExistingFile(DataFileName);

				var fileInfo = new FileInfo(DataFileName);

				using (var excelPackage = new ExcelPackage(fileInfo))
				using (var reader = SourceSqlCommand.ExecuteReader())
				{
					var ws = excelPackage.Workbook.Worksheets.Add("DataMover");

					WriteExcelHeader(ws);

					while (reader.Read())
					{
						recCnt++;

						WriteRowDynamic(reader, ws, recCnt);

						if (recCnt % LogRecordCountEvery == 0)
						{
							TraceLog.WriteConsole($"Records: {recCnt}");
						}
					}

					excelPackage.Save();
				}

				return recCnt;
			}
		}
	}
}
