﻿using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Reflection;
using System.Text;

namespace DataMover
{
	using Lines = List<string>;

	public static partial class DataMover
	{
		internal class TableCommandExport : TableCommand
		{
			private SqlCommand _sourceSqlCommand;
			private string _sourceSqlStatement;

			public TableCommandExport()
			{
			}

			public TableCommandExport(string databaseName, string tableName, string dataFileName, string path) : base(databaseName, tableName, dataFileName, path)
			{
			}

			protected override void ParseInner(Dictionary<string, Lines> keywords)
			{
				if (HasSqlStatement(keywords))
				{
					_sourceSqlStatement = string.Join("\n", keywords[KwdSql]);
				}
			}

			public override void Setup()
			{
				SetupNames();

				SetupSqlStatement();

				SetupSqlCommand();
				SetupTableColumns();

				GenerateDynamicClass();
			}

			private void SetupSqlStatement()
			{
				if (string.IsNullOrEmpty(_sourceSqlStatement))
				{
					_sourceSqlStatement = $"SELECT * FROM {FullyQualifiedTableName}";
				}

				TraceLog.WriteLine($"Export Sql statement: [{_sourceSqlStatement}]");
			}

			private void SetupSqlCommand()
			{
				_sourceSqlCommand = new SqlCommand(_sourceSqlStatement, GetSqlConnection()) { CommandTimeout = TimeoutSecRead };
			}

			protected override void SetupTableColumns()
			{
				TableColumns = _sourceSqlCommand.SetupColumns();
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
				TraceLog.Console($"Exporting table {FullyQualifiedTableName}");

				var WriteRowDynamic = (DynamicMethodDelegate)methodInfo.CreateDelegate(typeof(DynamicMethodDelegate));

				var recIdx = 0;

				using (var fileStream = new FileStream(DataFileName, FileMode.Create))
				using (var gZipStream = new GZipStream(fileStream, CompressionMode.Compress))
				using (var bufferedStream = new BufferedStream(gZipStream, BufferSize))
				using (var bw = new BinaryWriter(bufferedStream))
				using (var reader = _sourceSqlCommand.ExecuteReader())
				{
					WriteFileHeader(bw);

					while (reader.Read())
					{
						bw.Write(recIdx++);

						WriteRowDynamic(reader, bw);

						if (recIdx % LogRecordCountEvery == 0)
						{
							TraceLog.Console($"Records: {recIdx}");
						}
					}

					bw.Write(EofIndex);
				}

				return recIdx;
			}
		}
	}
}
