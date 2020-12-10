namespace DataMover
{
	public static partial class DataMover
	{
		internal class TableCommandDropFkConstraint : TableCommandStatement
		{
			protected override string ExecLogMessage => "Drop foreign key constraint from";
			protected override string DataFileType => "sql";

			private readonly string _constraintName;

			public TableCommandDropFkConstraint()
			{
			}

			public TableCommandDropFkConstraint(string databaseName, string tableName, string dataFileName, string path, string constraintName) : base(databaseName, tableName, dataFileName, path)
			{
				_constraintName = constraintName;
			}

			protected override void SetupSqlStatement()
			{
				SqlStatement = $"ALTER TABLE {FullyQualifiedTableName} DROP CONSTRAINT [{_constraintName}]";
			}
		}
	}
}