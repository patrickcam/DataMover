using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;

namespace DataMover
{
	using Lines = List<string>;

	public static partial class DataMover
	{
		public enum Status
		{
			Success = 0,
			Fail
		}

		internal abstract class Command
		{
			public Status Status;

			protected static SqlConnection SqlCcn;

			protected string ConnectionString;
			protected string ServerName;
			protected string DatabaseName;
			protected string DataFilePath;

			protected string SqlConstraintsFileName => $"{DatabaseName}_Add_Constraints.sql";
			protected string SqlSavedConstraintsFileName => $"{DatabaseName}_Add_Constraints_{DateTime.Now:yyyyMMdd_HHmmss}.sql";
			protected string SqlConstraintsFullFileName => Path.Combine(DataFilePath, SqlConstraintsFileName);
			protected string SqlSavedConstraintsFullFileName => Path.Combine(DataFilePath, SqlSavedConstraintsFileName);

			public abstract void Parse(Dictionary<string, Lines> keywords);

			public abstract void Setup();

			public abstract int Execute();

			public SqlConnection GetSqlConnection()
			{
				return SqlCcn ?? (SqlCcn = SetupSqlConnection());
			}

			public SqlConnection SetupSqlConnection()
			{
				if (string.IsNullOrEmpty(ConnectionString))
				{
					throw new TraceLog.InternalException("Missing connection string");
				}

				var sqlConnection = new SqlConnection(ConnectionString);

				sqlConnection.Open();

				return sqlConnection;
			}

			protected void SetupConnectionString()
			{
				//const string kwdServerName = "Data Source";
				//const string kwdIntegratedSecurity = "Integrated Security";
				//const string kwdDatabaseName = "Initial Catalog";

				//var builder = new SqlConnectionStringBuilder(ConnectionString)
				//{
				//	[kwdServerName] = string.IsNullOrEmpty(ServerName) ? "(local)" : ServerName,
				//	[kwdIntegratedSecurity] = true,
				//	[kwdDatabaseName] = DatabaseName
				//};

				var builder = new SqlConnectionStringBuilder(ConnectionString)
				{
					IntegratedSecurity = true
				};

				if (string.IsNullOrEmpty(builder.DataSource))
				{
					if (string.IsNullOrEmpty(ServerName))
					{
						ServerName = "(local)";
					}

					builder.DataSource = ServerName;
				}

				if (string.IsNullOrEmpty(builder.InitialCatalog))
				{
					builder.InitialCatalog = DatabaseName;
				}

				ConnectionString = builder.ConnectionString;

				TraceLog.WriteConsole(ConnectionString);
			}

			protected void SaveExistingFile(string fullFileName)
			{
				var fileInfo = new FileInfo(fullFileName);
				if (fileInfo.Exists)
				{
					var lastmodified = fileInfo.LastWriteTime;

					var directoryName = Path.GetDirectoryName(fullFileName);
					var fileName = Path.GetFileNameWithoutExtension(fullFileName);
					var fileExtension = Path.GetExtension(fullFileName);

					var savedFileName = $"{fileName}_{lastmodified:yyyyMMdd_HHmmss}.{fileExtension}";
					var savedFullFileNAme = Path.Combine(directoryName, savedFileName);

					TraceLog.Info($"Saving existing file to [{savedFullFileNAme}]");
					File.Move(fullFileName, savedFullFileNAme);
				}
			}
		}
	}
}