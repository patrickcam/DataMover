using System.Data.SqlClient;
using System.Reflection;

namespace DataMover
{
	public static partial class DataMover
	{
		internal abstract class TableCommandStatement : TableCommand
		{
			protected SqlCommand SqlCommand;
			protected string SqlStatement;

			protected abstract string ExecLogMessage { get; }

			protected TableCommandStatement()
			{
			}

			protected TableCommandStatement(string databaseName, string tableName, string dataFileName, string path) : base(databaseName, tableName, dataFileName, path)
			{
			}

			public override void Setup()
			{
				SetupNames();
				SetupSqlStatement();
				SetupSqlCommand();
			}

			protected abstract void SetupSqlStatement();

			protected virtual void SetupSqlCommand()
			{
				SqlCommand = new SqlCommand(SqlStatement, GetSqlConnection()) { CommandTimeout = TimeoutSecRead };
			}

			public override int ExecInner(MethodInfo methodInfo)
			{
				TraceLog.WriteConsole($"{ExecLogMessage} table {FullyQualifiedTableName}");

				var cnt = SqlCommand.ExecuteNonQuery();

				return cnt;
			}
		}
	}
}
