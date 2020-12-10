namespace DataMover
{
	public static partial class DataMover
	{
		internal class TableCommandDelete : TableCommandStatement
		{
			protected override string ExecLogMessage => "Delete";
			protected override string DataFileType => string.Empty;

			public TableCommandDelete()
			{
			}

			public TableCommandDelete(string databaseName, string tableName, string dataFileName, string path) : base(databaseName, tableName, dataFileName, path)
			{
			}

			protected override void SetupSqlStatement()
			{
				SqlStatement = $"DELETE FROM {FullyQualifiedTableName}";
			}
		}
	}
}
