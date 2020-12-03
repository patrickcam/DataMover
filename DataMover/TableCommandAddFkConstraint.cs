namespace DataMover
{
	public static partial class DataMover
	{
		internal class TableCommandAddFkConstraint : TableCommandStatement
		{
			protected override string Keyword => "Create foreign key constraint for";

			public TableCommandAddFkConstraint()
			{
			}

			public TableCommandAddFkConstraint(string databaseName, string tableName, string dataFileName, string path,
				string sqlStatement) : base(databaseName, tableName, dataFileName, path)
			{
				SqlStatement = sqlStatement;
			}

			protected override void SetupSqlStatement()
			{
			}
		}
	}
}