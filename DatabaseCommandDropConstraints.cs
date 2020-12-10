using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Text;

namespace DataMover
{
	public class Constraint
	{
		public string SchemaName;
		public string TableName;
		public string FkName;
		public string FkColNames;
		public string RefSchemaName;
		public string RefTableName;
		public string RefColNames;
	}

	public static partial class DataMover
	{
		internal class DatabaseCommandDropConstraints : DatabaseCommand
		{
			public DatabaseCommandDropConstraints(CommandDatabaseOpc opc) : base(opc)
			{
			}

			public override void Setup()
			{
				SaveAddConstraintStatements();

				//!DBG!
				//SetupDropConstraintStatements();

				Commands.ForEach(c => c.Setup());
			}

			public static List<Constraint> GetFkConstraints(SqlConnection sqlCcn, string databaseName)
			{
				var constraints = new List<Constraint>(20);

				const string stmt = @"
SELECT 
	[cs].[name] [SchemaName], 
	[ct].[name] [TableName], 
	[fk].[name] [FkName], 
	STUFF(
(
	 SELECT 
		 ',' + QUOTENAME([c].[name])
	 FROM 
		 [sys].[columns] AS [c]
		 JOIN [sys].[foreign_key_columns] AS [fkc] ON [fkc].[parent_column_id] = [c].[column_id]
																	 AND [fkc].[parent_object_id] = [c].[object_id]
	 WHERE [fkc].[constraint_object_id] = [fk].[object_id]
	 ORDER BY 
		 [fkc].[constraint_column_id] FOR
	 XML PATH(N''), TYPE
).value(N'.[1]', N'nvarchar(max)'), 1, 1, N'') [FkColNames], 
	[rs].[name] [RefSchemaName], 
	[rt].[name] [RefTableName], 
	STUFF(
(
	 SELECT 
		 ',' + QUOTENAME([c].[name])
	 FROM 
		 [sys].[columns] AS [c]
		 JOIN [sys].[foreign_key_columns] AS [fkc] ON [fkc].[referenced_column_id] = [c].[column_id]
																	 AND [fkc].[referenced_object_id] = [c].[object_id]
	 WHERE [fkc].[constraint_object_id] = [fk].[object_id]
	 ORDER BY 
		 [fkc].[constraint_column_id] FOR
	 XML PATH(N''), TYPE
).value(N'.[1]', N'nvarchar(max)'), 1, 1, N'') [RefColNames]
FROM 
	[sys].[foreign_keys] AS [fk]
	JOIN [sys].[tables] AS [rt] ON [fk].[referenced_object_id] = [rt].[object_id]
	JOIN [sys].[schemas] AS [rs] ON [rt].[schema_id] = [rs].[schema_id]
	JOIN [sys].[tables] AS [ct] ON [fk].[parent_object_id] = [ct].[object_id]
	JOIN [sys].[schemas] AS [cs] ON [ct].[schema_id] = [cs].[schema_id]
WHERE [rt].[is_ms_shipped] = 0
		AND [ct].[is_ms_shipped] = 0";

				using (var useCmd = new SqlCommand(stmt, sqlCcn, null) { CommandTimeout = TimeoutSecRead })
				using (var useReader = useCmd.ExecuteReader())
				{
					var idxSchemaName = useReader.GetOrdinal("SchemaName");
					var idxTableName = useReader.GetOrdinal("TableName");
					var idxFkName = useReader.GetOrdinal("FkName");
					var idxFkColNames = useReader.GetOrdinal("FkColNames");
					var idxRefSchemaName = useReader.GetOrdinal("RefSchemaName");
					var idxRefTableName = useReader.GetOrdinal("RefTableName");
					var idxRefColNames = useReader.GetOrdinal("RefColNames");

					while (useReader.Read())
					{
						var constraint = new Constraint
						{
							SchemaName = useReader.GetString(idxSchemaName),
							TableName = useReader.GetString(idxTableName),
							FkName = useReader.GetString(idxFkName),
							FkColNames = useReader.GetString(idxFkColNames),
							RefSchemaName = useReader.GetString(idxRefSchemaName),
							RefTableName = useReader.GetString(idxRefTableName),
							RefColNames = useReader.GetString(idxRefColNames)
						};

						constraints.Add(constraint);
					}
				}

				return constraints;
			}

			private void SaveAddConstraintStatements()
			{
				const string stmt = @"
SELECT 
	[cs].[name] SchemaName,
	[ct].[name] TableName,
	N'ALTER TABLE ' + QUOTENAME([cs].[name]) + '.' + QUOTENAME([ct].[name]) + ' ADD CONSTRAINT ' + QUOTENAME([fk].[name]) + ' FOREIGN KEY (' + STUFF(
(
	 SELECT 
		 ',' + QUOTENAME([c].[name])
	 FROM 
		 [sys].[columns] AS [c]
		 JOIN [sys].[foreign_key_columns] AS [fkc] ON [fkc].[parent_column_id] = [c].[column_id] AND [fkc].[parent_object_id] = [c].[object_id]
	 WHERE [fkc].[constraint_object_id] = [fk].[object_id]
	 ORDER BY 
		 [fkc].[constraint_column_id] FOR
	 XML PATH(N''), TYPE).value(N'.[1]', N'nvarchar(max)'), 1, 1, N'') + ') REFERENCES ' + QUOTENAME([rs].[name]) + '.' + QUOTENAME([rt].[name]) + '(' + STUFF(
(
	 SELECT 
		 ',' + QUOTENAME([c].[name])
	 FROM 
		 [sys].[columns] AS [c]
		 JOIN [sys].[foreign_key_columns] AS [fkc] ON [fkc].[referenced_column_id] = [c].[column_id] AND [fkc].[referenced_object_id] = [c].[object_id]
	 WHERE [fkc].[constraint_object_id] = [fk].[object_id]
	 ORDER BY 
		 [fkc].[constraint_column_id] FOR
	 XML PATH(N''), TYPE
).value(N'.[1]', N'nvarchar(max)'), 1, 1, N'') + ');'
FROM 
	[sys].[foreign_keys] AS [fk]
	JOIN [sys].[tables] AS [rt]
	ON [fk].[referenced_object_id] = [rt].[object_id]
	JOIN [sys].[schemas] AS [rs] ON [rt].[schema_id] = [rs].[schema_id]
	JOIN [sys].[tables] AS [ct]
	ON [fk].[parent_object_id] = [ct].[object_id]
	JOIN [sys].[schemas] AS [cs] ON [ct].[schema_id] = [cs].[schema_id]
WHERE [rt].[is_ms_shipped] = 0 AND [ct].[is_ms_shipped] = 0;";

				var sb = new StringBuilder();

				using (var useCmd = new SqlCommand(stmt, GetSqlConnection(), null) { CommandTimeout = TimeoutSecRead })
				using (var useReader = useCmd.ExecuteReader())
				{
					while (useReader.Read())
					{
						sb.AppendLine(useReader.GetString(2));
					}
				}

				if (sb.Length > 0)
				{
					SaveExistingFile(SqlConstraintsFullFileName);

					TraceLog.WriteConsole("Writing constraint creation SQL script");
					File.WriteAllText(SqlConstraintsFullFileName, sb.ToString());
				}
				else
				{
					TraceLog.WriteConsole($"No constraints found in database {DatabaseName}");
				}
			}

			private void SetupDropConstraintStatements()
			{
				const string stmt = @"
SELECT 
	[cs].[name] SchemaName,
	[ct].[name] TableName,
	[fk].[name] FkName
FROM 
	[sys].[foreign_keys] AS [fk]
	JOIN [sys].[tables] AS [ct] ON [fk].[parent_object_id] = [ct].[object_id]
	JOIN [sys].[schemas] AS [cs] ON [ct].[schema_id] = [cs].[schema_id];";

				using (var useCmd = new SqlCommand(stmt, GetSqlConnection(), null) { CommandTimeout = TimeoutSecRead })
				using (var useReader = useCmd.ExecuteReader())
				{
					while (useReader.Read())
					{
						//TODO Add schema name
						var tableCommand = new TableCommandDropFkConstraint(
							DatabaseName,
							useReader.GetString(1),
							string.Empty,
							string.Empty,
							useReader.GetString(2));

						Commands.Add(tableCommand);
					}
				}
			}
		}
	}
}