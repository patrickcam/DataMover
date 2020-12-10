using System.Collections.Generic;
using System.Data.SqlClient;

namespace DataMover
{
	using Lines = List<string>;

	public static partial class DataMover
	{
		private abstract class TableCommandExport : TableCommand
		{
			protected SqlCommand SourceSqlCommand;
			private string _sourceSqlStatement;

			protected TableCommandExport()
			{
			}

			protected TableCommandExport(string databaseName, string tableName, string dataFileName, string path) : base(databaseName, tableName, dataFileName, path)
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
				SourceSqlCommand = new SqlCommand(_sourceSqlStatement, GetSqlConnection()) { CommandTimeout = TimeoutSecRead };
			}

			protected override void SetupTableColumns()
			{
				TableColumns = SourceSqlCommand.SetupColumns();
			}
		}
	}
}
