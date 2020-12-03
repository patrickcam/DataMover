﻿using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Text;

namespace DataMover
{
	using Lines = List<string>;

	public static partial class DataMover
	{
		internal class TableCommandImport : TableCommand
		{
			private DataTable _destDataTable;
			private readonly List<SqlBulkCopyColumnMapping> _columnMappings = new List<SqlBulkCopyColumnMapping>(100);

			public TableCommandImport()
			{
			}

			public TableCommandImport(string databaseName, string tableName, string dataFileName, string path) : base(databaseName, tableName, dataFileName, path)
			{
			}

			protected override void ParseInner(Dictionary<string, Lines> keywords)
			{
			}

			public override void Setup()
			{
				SetupNames();

				SetupTableColumns();
				SetupDestDataTable();

				ReadFileHeader();
				MatchColumns();
				SetupColumnMappings();

				GenerateDynamicClass();
			}

			private void SetupDestDataTable()
			{
				_destDataTable = ExtensionsDataTable.SetupForInsert(FullyQualifiedTableName, GetSqlConnection(), TableColumns);
			}

			protected override void SetupTableColumns()
			{
				var stmt = $"SELECT TOP 0 * FROM {FullyQualifiedTableName}";
				using (var useCmd = new SqlCommand(stmt, GetSqlConnection()) { CommandTimeout = TimeoutSecRead })
				{
					TableColumns = useCmd.SetupColumns();
				}
			}

			private void SetupColumnMappings()
			{
				if (!TableColumns.Any(c => c.NoInsert))
				{
					return;
				}

				TraceLog.WriteLine("Column Mappings");

				foreach (var col in TableColumns)
				{
					if (!col.NoInsert)
					{
						var destOrdinal = col.GetDestDataTableOrdinal(_destDataTable);
						var mapping = new SqlBulkCopyColumnMapping(destOrdinal, col.TableOrdinal);
						_columnMappings.Add(mapping);

						TraceLog.WriteLine($"\t[{col.Name}] {destOrdinal} -> {col.TableOrdinal}");
					}
				}
			}

			private void MatchColumns()
			{
				var headerColumns = HeaderColumns.ToDictionary(c => c.Name, c => c);

				foreach (var tableColumn in TableColumns)
				{
					if (headerColumns.TryGetValue(tableColumn.Name, out var matchedHeaderColumn))
					{
						tableColumn.MatchedColumn = matchedHeaderColumn;
						matchedHeaderColumn.MatchedColumn = tableColumn;
					}

					TraceLog.WriteLine($"Import table column: [{tableColumn.Name}], {(!tableColumn.IsMatched ? "no-" : null)}match");
				}

				foreach (var headerColumn in HeaderColumns)
				{
					if (!headerColumn.IsMatched)
					{
						TraceLog.WriteLine($"Import header column: [{headerColumn.Name}], no-match");
					}
				}
			}

			protected override void GenDynamicMethod(StringBuilder sb)
			{
				sb.AppendLine(@"public static void DynamicMethod(DataRow newRow, BinaryReader br)");
				sb.AppendLine("{");

				HeaderColumns.ForEach(c => c.GenReadBinary(sb, _destDataTable));
				TableColumns.ForEach(c => c.GenWriteDefaultValueIfNotMatched(sb));

				sb.AppendLine("}");
			}

			private delegate void DynamicMethodDelegate(DataRow newRow, BinaryReader br);

			public override int ExecInner(MethodInfo methodInfo)
			{
				TraceLog.Console($"Importing table {FullyQualifiedTableName}");

				var ReadRowDynamic = (DynamicMethodDelegate)methodInfo.CreateDelegate(typeof(DynamicMethodDelegate));

				var batchRecCnt = 0;
				var readRecIdx = -1;

				using (var fileStream = new FileStream(DataFileName, FileMode.Open))
				using (var gZipStream = new GZipStream(fileStream, CompressionMode.Decompress))
				using (var bufferedStream = new BufferedStream(gZipStream, BufferSize))
				using (var br = new BinaryReader(bufferedStream))
				{
					SkipFileHeader(br);

					while (true)
					{
						readRecIdx++;
						batchRecCnt++;

						var recIdx = br.ReadInt32();
						if (recIdx == EofIndex)
						{
							break;
						}
						if (recIdx != readRecIdx)
						{
							throw new TraceLog.InternalException($"Record index out of sync, expected:{readRecIdx}, received:{recIdx}");
						}

						var newRow = _destDataTable.NewRow();

						ReadRowDynamic(newRow, br);

						_destDataTable.Rows.Add(newRow);

						if (batchRecCnt == CommitEvery)
						{
							WriteBatch(batchRecCnt, readRecIdx);
							batchRecCnt = 0;
						}
					}

					if (batchRecCnt != 0)
					{
						WriteBatch(batchRecCnt, readRecIdx);
					}

					return readRecIdx + 1;
				}
			}

			private void WriteBatch(int batchRecCnt, int readRecIdx)
			{
				TraceLog.Console($"Commit: {batchRecCnt}, records: {readRecIdx + 1}");

				using (var sqlTxn = GetSqlConnection().BeginTransaction())
				{
					_destDataTable.WriteToServer(sqlTxn, _columnMappings);
					sqlTxn.Commit();

					_destDataTable.Clear();
				}
			}
		}
	}
}