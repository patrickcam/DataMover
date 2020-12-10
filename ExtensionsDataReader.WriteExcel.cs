using OfficeOpenXml;
using System;
using System.Data.SqlClient;
using System.Globalization;

namespace DataMover
{
	public static partial class ExtensionsDataReader
	{
		private static bool WriteExcelNullFlag(SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			var isNotNull = !reader.IsDBNull(idx);
			return isNotNull;
		}

		public static void WriteExcelBytes(this SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			throw new NotImplementedException();
		}

		public static void WriteExcelBytesNullable(this SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			throw new NotImplementedException();
		}

		public static void WriteExcelByte(this SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			ws.Cells[recIdx + 1, idx + 1].Value = reader.GetByte(idx);
		}

		public static void WriteExcelByteNullable(this SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			if (WriteExcelNullFlag(reader, idx, ws, recIdx))
			{
				WriteExcelByte(reader, idx, ws, recIdx);
			}
		}

		public static void WriteExcelBoolean(this SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			ws.Cells[recIdx + 1, idx + 1].Value = reader.GetBoolean(idx);
		}

		public static void WriteExcelBooleanNullable(this SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			if (WriteExcelNullFlag(reader, idx, ws, recIdx))
			{
				WriteExcelBoolean(reader, idx, ws, recIdx);
			}
		}

		public static void WriteExcelInt16(this SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			ws.Cells[recIdx + 1, idx + 1].Value = reader.GetInt16(idx);
		}

		public static void WriteExcelInt16Nullable(this SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			if (WriteExcelNullFlag(reader, idx, ws, recIdx))
			{
				WriteExcelInt16(reader, idx, ws, recIdx);
			}
		}

		public static void WriteExcelInt32(this SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			ws.Cells[recIdx + 1, idx + 1].Value = reader.GetInt32(idx);
		}

		public static void WriteExcelInt32Nullable(this SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			if (WriteExcelNullFlag(reader, idx, ws, recIdx))
			{
				WriteExcelInt32(reader, idx, ws, recIdx);
			}
		}

		public static void WriteExcelInt64(this SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			ws.Cells[recIdx + 1, idx + 1].Value = reader.GetInt64(idx);
		}

		public static void WriteExcelInt64Nullable(this SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			if (WriteExcelNullFlag(reader, idx, ws, recIdx))
			{
				WriteExcelInt64(reader, idx, ws, recIdx);
			}
		}

		public static void WriteExcelDateTime(this SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			ws.Cells[recIdx + 1, idx + 1].Value = reader.GetDateTime(idx).ToString(CultureInfo.CurrentCulture);
		}

		public static void WriteExcelDateTimeNullable(this SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			if (WriteExcelNullFlag(reader, idx, ws, recIdx))
			{
				WriteExcelDateTime(reader, idx, ws, recIdx);
			}
		}

		public static void WriteExcelDateTimeOffset(this SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			var dateTimeOffset = reader.GetDateTimeOffset(idx);
			var dateTime = dateTimeOffset.DateTime;
			var minutes = (short)dateTimeOffset.Offset.TotalMinutes;

			ws.Cells[recIdx + 1, idx + 1].Value = dateTime.ToString(CultureInfo.CurrentCulture) + minutes.ToString();
		}

		public static void WriteExcelDateTimeOffsetNullable(this SqlDataReader reader, int idx, ExcelWorksheet ws,
			int recIdx)
		{
			if (WriteExcelNullFlag(reader, idx, ws, recIdx))
			{
				WriteExcelDateTimeOffset(reader, idx, ws, recIdx);
			}
		}

		public static void WriteExcelDecimal(this SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			ws.Cells[recIdx + 1, idx + 1].Value = reader.GetDecimal(idx);
		}

		public static void WriteExcelDecimalNullable(this SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			if (WriteExcelNullFlag(reader, idx, ws, recIdx))
			{
				WriteExcelDecimal(reader, idx, ws, recIdx);
			}
		}

		public static void WriteExcelString(this SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			var value = reader.GetString(idx);
			if (!string.IsNullOrEmpty(value))
			{
				ws.Cells[recIdx + 1, idx + 1].Value = value;
			}
		}

		public static void WriteExcelStringNullable(this SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			if (WriteExcelNullFlag(reader, idx, ws, recIdx))
			{
				WriteExcelString(reader, idx, ws, recIdx);
			}
		}

		public static void WriteExcelGuid(this SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			ws.Cells[recIdx + 1, idx + 1].Value = reader.GetGuid(idx).ToString();
		}

		public static void WriteExcelGuidNullable(this SqlDataReader reader, int idx, ExcelWorksheet ws, int recIdx)
		{
			if (WriteExcelNullFlag(reader, idx, ws, recIdx))
			{
				WriteExcelGuid(reader, idx, ws, recIdx);
			}
		}
	}
}