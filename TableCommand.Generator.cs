using System.Text;

namespace DataMover
{
	public static partial class DataMover
	{
		internal partial class TableCommand
		{
			protected virtual void GenDynamicMethod(StringBuilder sb)
			{
			}

			protected void GenerateDynamicClass()
			{
				var sb = new StringBuilder();

				sb.AppendLine(
					@"using System;
				using System.IO;
				using System.Data;
				using System.Data.SqlClient;
				using DataMover;
				using OfficeOpenXml;
				public static class GeneratedClass
				{");

				GenDynamicMethod(sb);

				sb.AppendLine("}");

				var generatedCode = sb.ToString();

				DynamicMethod = Compile(generatedCode);
			}
		}
	}
}