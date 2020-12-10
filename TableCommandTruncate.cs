namespace DataMover
{
	public static partial class DataMover
	{
		internal class TableCommandTruncate : TableCommandStatement
		{
			protected override string ExecLogMessage => "Truncate";
			protected override string DataFileType => "sql";

			public TableCommandTruncate()
			{
			}

			public TableCommandTruncate(string databaseName, string tableName, string dataFileName, string path) : base(databaseName, tableName, dataFileName, path)
			{
			}

			protected override void SetupSqlStatement()
			{
				SqlStatement = $"TRUNCATE TABLE {FullyQualifiedTableName}";
			}
		}
	}
}