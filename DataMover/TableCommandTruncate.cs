﻿namespace DataMover
{
	public static partial class DataMover
	{
		internal class TableCommandTruncate : TableCommandStatement
		{
			protected override string Keyword => "Truncate";

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