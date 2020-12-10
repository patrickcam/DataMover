using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace DataMover
{
	using Lines = List<string>;

	public static partial class DataMover
	{
		internal class TableCommandImport : TableCommand
		{
			private DataTable _destDataTable;
			private readonly List<SqlBulkCopyColumnMapping> _columnMappings = new List<SqlBulkCopyColumnMapping>(100);
			protected override string DataFileType => "dat";

			private readonly List<TableCommandDropFkConstraint> _commandsDropFkConstraints = new List<TableCommandDropFkConstraint>(20);
			private readonly List<TableCommandAddFkConstraint> _commandsAddFkConstraints = new List<TableCommandAddFkConstraint>(20);

			private TableCommandTruncate _commandTruncate;

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

				ReadBinaryHeader();
				MatchColumns();
				SetupColumnMappings();

				SetupCommandsFkConstraints();
				SetupCommandTruncate();

				GenerateDynamicClass();
			}

			private void SetupCommandTruncate()
			{
				_commandTruncate = new TableCommandTruncate(DatabaseName, TableName, string.Empty, string.Empty);
				_commandTruncate.Setup();
			}

			private void SetupCommandsFkConstraints()
			{
				if (DatabaseConstraints == null)
				{
					DatabaseConstraints = DatabaseCommandDropConstraints.GetFkConstraints(GetSqlConnection(), DatabaseName);
				}

				foreach (var constraint in DatabaseConstraints)
				{
					if (constraint.RefTableName == TableName)
					{
						var dropFkConstraint = new TableCommandDropFkConstraint(
							databaseName: DatabaseName,
							tableName: constraint.TableName,
							dataFileName: string.Empty,
							path: string.Empty,
							constraintName: constraint.FkName);

						_commandsDropFkConstraints.Add(dropFkConstraint);

						//!TODO Move to class
						var sqlStatement = $"ALTER TABLE {constraint.TableName} ADD CONSTRAINT {constraint.FkName} FOREIGN KEY ({constraint.FkColNames}) REFERENCES {constraint.RefTableName} ({constraint.RefColNames})";

						var addFkConstraint = new TableCommandAddFkConstraint(
							DatabaseName,
							constraint.TableName,
							string.Empty,
							string.Empty,
							sqlStatement);

						_commandsAddFkConstraints.Add(addFkConstraint);
					}
				}

				_commandsDropFkConstraints.ForEach(c => c.Setup());
				_commandsAddFkConstraints.ForEach(c => c.Setup());
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
				var sourceColumns = SourceColumns.ToDictionary(c => c.Name, c => c);

				foreach (var tableColumn in TableColumns)
				{
					if (sourceColumns.TryGetValue(tableColumn.Name, out var matchedHeaderColumn))
					{
						tableColumn.MatchedColumn = matchedHeaderColumn;
						matchedHeaderColumn.MatchedColumn = tableColumn;
					}

					TraceLog.WriteLine($"Import table column: [{tableColumn.Name}], {(!tableColumn.IsMatched ? "no-" : null)}match");
				}

				foreach (var sourceColumn in SourceColumns)
				{
					if (!sourceColumn.IsMatched)
					{
						TraceLog.WriteLine($"Import source column: [{sourceColumn.Name}], no-match");
					}
				}
			}

			protected override void GenDynamicMethod(StringBuilder sb)
			{
				sb.AppendLine(@"public static void DynamicMethod(DataRow dataRow, BinaryReader br)");
				sb.AppendLine("{");

				SourceColumns.ForEach(c => c.GenReadBinary(sb, _destDataTable));
				TableColumns.ForEach(c => c.GenWriteDefaultValueIfNotMatched(sb));

				sb.AppendLine("}");
			}

			private delegate void DynamicMethodDelegate(DataRow dataRow, BinaryReader br);

			public override int ExecInner(MethodInfo methodInfo)
			{
				TraceLog.WriteConsole($"Importing table {FullyQualifiedTableName}");

				var ReadRowDynamic = (DynamicMethodDelegate)methodInfo.CreateDelegate(typeof(DynamicMethodDelegate));

				_commandsDropFkConstraints.ForEach(c => c.Execute());
				_commandTruncate.Execute();

				var readRecCnt = 0;
				var batchRecCnt = 0;

				var batch1 = new DataRow[CommitEvery];
				var batch2 = new DataRow[CommitEvery];
				var curBatch = batch1;

				for (var idx = 0; idx < CommitEvery; idx++)
				{
					batch1[idx] = _destDataTable.NewRow();
				}
				for (var idx = 0; idx < CommitEvery; idx++)
				{
					batch2[idx] = _destDataTable.NewRow();
				}

				Task writeTask = null;

				using (var fileStream = new FileStream(DataFileName, FileMode.Open))
				using (var gZipStream = new GZipStream(fileStream, CompressionMode.Decompress))
				using (var bufferedStream = new BufferedStream(gZipStream, BufferSize))
				using (var br = new BinaryReader(bufferedStream))
				{
					SkipBinaryHeader(br);

					while (true)
					{
						var recIdx = br.ReadInt32();
						if (recIdx == EofIndex)
						{
							break;
						}
						if (recIdx != readRecCnt)
						{
							throw new TraceLog.InternalException($"Record index out of sync, expected:{readRecCnt}, received:{recIdx}");
						}

						var curRow = curBatch[batchRecCnt];

						ReadRowDynamic(curRow, br);

						readRecCnt++;
						batchRecCnt++;

						if (batchRecCnt == CommitEvery)
						{
							writeTask?.Wait();
							writeTask = WriteBatch(curBatch, batchRecCnt, readRecCnt);

							curBatch = curBatch == batch1 ? batch2 : batch1;
							batchRecCnt = 0;
						}
					}

					if (batchRecCnt != 0)
					{
						writeTask?.Wait();
						writeTask = WriteBatch(curBatch, batchRecCnt, readRecCnt);
						writeTask.Wait();
					}

					_commandsAddFkConstraints.ForEach(c => c.Execute());

					return readRecCnt;
				}
			}

			private async Task WriteBatch(DataRow[] batch, int batchRecCnt, int readRecCnt)
			{
				TraceLog.WriteConsole($"Commit: {batchRecCnt}, records: {readRecCnt}");

				if (batchRecCnt < CommitEvery)
				{
					Array.Resize(ref batch, batchRecCnt);
				}

				using (var sqlTxn = GetSqlConnection().BeginTransaction())
				using (var useCpy = new SqlBulkCopy(sqlTxn.Connection, DefaultSqlBulkCopyOptions, sqlTxn)
				{
					DestinationTableName = TableName,
					BulkCopyTimeout = TimeoutSecWrite,
					BatchSize = DefaultSqlBulkCopyBatchSize
				})
				{
					foreach (var mapping in _columnMappings)
					{
						useCpy.ColumnMappings.Add(mapping);
					}

					await useCpy.WriteToServerAsync(batch);

					sqlTxn.Commit();
				}
			}

			private async Task WriteBatch(int batchRecCnt, int readRecIdx)
			{
				TraceLog.WriteConsole($"Commit: {batchRecCnt}, records: {readRecIdx + 1}");

				using (var sqlTxn = GetSqlConnection().BeginTransaction())
				{
					await _destDataTable.WriteToServer(sqlTxn, _columnMappings);

					sqlTxn.Commit();

					_destDataTable.Clear();
				}
			}
		}
	}
}