using Microsoft.CSharp;
using OfficeOpenXml;
using System;
using System.CodeDom.Compiler;
using System.Reflection;

namespace DataMover
{
	public static partial class DataMover
	{
		public static MethodInfo Compile(string generatedCode)
		{
			TraceLog.Section("Generated code");
			TraceLog.WriteLine(generatedCode);
			TraceLog.Section();

			var provider = new CSharpCodeProvider();

			// If compiler version is required:
			//var provider = new CSharpCodeProvider(new Dictionary<string, string>() { { "CompilerVersion", "v4.8" } });

			var parameters = new CompilerParameters
			{
				TempFiles = new TempFileCollection(Environment.GetEnvironmentVariable("TEMP"), true),
				GenerateInMemory = true,
				GenerateExecutable = false,
				IncludeDebugInformation = true
				//CompilerOptions = "/optimize+"
			};

			var hostAssembly = Assembly.GetExecutingAssembly();
			parameters.ReferencedAssemblies.Add(hostAssembly.Location);

			parameters.ReferencedAssemblies.Add("System.dll");
			parameters.ReferencedAssemblies.Add("System.Data.dll");
			parameters.ReferencedAssemblies.Add("System.Xml.dll");

			// Avoid: Error (CS0006): Metadata file 'EPPlus.dll' could not be found
			parameters.ReferencedAssemblies.Add(typeof(ExcelPackage).Assembly.Location);

			var results = provider.CompileAssemblyFromSource(parameters, generatedCode);

			if (results.Errors.HasErrors)
			{
				foreach (CompilerError error in results.Errors)
				{
					TraceLog.Error($"Error ({error.ErrorNumber}): {error.ErrorText}");
				}

				throw new TraceLog.InternalException("Compilation failed");
			}

			var assembly = results.CompiledAssembly;
			var typeName = "GeneratedClass";
			var type = assembly.GetType(typeName);
			if (type == null)
			{
				TraceLog.Error($"No such type: [{typeName}]");
				throw new TraceLog.InternalException("Compilation failed");
			}

			var methodName = "DynamicMethod";
			var dynamicMethod = type.GetMethod(methodName);
			if (dynamicMethod == null)
			{
				TraceLog.Error($"No such method: [{methodName}]");
				throw new TraceLog.InternalException("Compilation failed");
			}

			return dynamicMethod;
		}
	}
}
