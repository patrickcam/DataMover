using System;
using System.Data.SqlClient;
using System.Globalization;

namespace DataMover
{
	public static partial class ExtensionsDataReader
	{
		private static bool WriteHtmlNullFlag(SqlDataReader reader, int idx)
		{
			var isNotNull = !reader.IsDBNull(idx);
			if (!isNotNull)
			{
				Html.Cell();
			}
			return isNotNull;
		}

		public static void WriteHtmlBytes(this SqlDataReader reader, int idx)
		{
			throw new NotImplementedException();
		}

		public static void WriteHtmlBytesNullable(this SqlDataReader reader, int idx)
		{
			throw new NotImplementedException();
		}

		public static void WriteHtmlByte(this SqlDataReader reader, int idx)
		{
			Html.Cell(reader.GetByte(idx));
		}

		public static void WriteHtmlByteNullable(this SqlDataReader reader, int idx)
		{
			if (WriteHtmlNullFlag(reader, idx))
			{
				WriteHtmlByte(reader, idx);
			}
		}

		public static void WriteHtmlBoolean(this SqlDataReader reader, int idx)
		{
			Html.Cell(reader.GetBoolean(idx));
		}

		public static void WriteHtmlBooleanNullable(this SqlDataReader reader, int idx)
		{
			if (WriteHtmlNullFlag(reader, idx))
			{
				WriteHtmlBoolean(reader, idx);
			}
		}

		public static void WriteHtmlInt16(this SqlDataReader reader, int idx)
		{
			Html.Cell(reader.GetInt16(idx));
		}

		public static void WriteHtmlInt16Nullable(this SqlDataReader reader, int idx)
		{
			if (WriteHtmlNullFlag(reader, idx))
			{
				WriteHtmlInt16(reader, idx);
			}
		}

		public static void WriteHtmlInt32(this SqlDataReader reader, int idx)
		{
			Html.Cell(reader.GetInt32(idx));
		}

		public static void WriteHtmlInt32Nullable(this SqlDataReader reader, int idx)
		{
			if (WriteHtmlNullFlag(reader, idx))
			{
				WriteHtmlInt32(reader, idx);
			}
		}

		public static void WriteHtmlInt64(this SqlDataReader reader, int idx)
		{
			Html.Cell(reader.GetInt64(idx));
		}

		public static void WriteHtmlInt64Nullable(this SqlDataReader reader, int idx)
		{
			if (WriteHtmlNullFlag(reader, idx))
			{
				WriteHtmlInt64(reader, idx);
			}
		}

		public static void WriteHtmlDateTime(this SqlDataReader reader, int idx)
		{
			Html.Cell(reader.GetDateTime(idx).ToString(CultureInfo.CurrentCulture));
		}

		public static void WriteHtmlDateTimeNullable(this SqlDataReader reader, int idx)
		{
			if (WriteHtmlNullFlag(reader, idx))
			{
				WriteHtmlDateTime(reader, idx);
			}
		}

		public static void WriteHtmlDateTimeOffset(this SqlDataReader reader, int idx)
		{
			var dateTimeOffset = reader.GetDateTimeOffset(idx);
			var dateTime = dateTimeOffset.DateTime;
			var minutes = (short)dateTimeOffset.Offset.TotalMinutes;

			Html.Cell(dateTime.ToString(CultureInfo.CurrentCulture) + minutes.ToString());
		}

		public static void WriteHtmlDateTimeOffsetNullable(this SqlDataReader reader, int idx,
			int recIdx)
		{
			if (WriteHtmlNullFlag(reader, idx))
			{
				WriteHtmlDateTimeOffset(reader, idx);
			}
		}

		public static void WriteHtmlDecimal(this SqlDataReader reader, int idx)
		{
			Html.Cell(reader.GetDecimal(idx));
		}

		public static void WriteHtmlDecimalNullable(this SqlDataReader reader, int idx)
		{
			if (WriteHtmlNullFlag(reader, idx))
			{
				WriteHtmlDecimal(reader, idx);
			}
		}

		public static void WriteHtmlString(this SqlDataReader reader, int idx)
		{
			var value = reader.GetString(idx);
			if (!string.IsNullOrEmpty(value))
			{
				Html.Cell(value);
			}
		}

		public static void WriteHtmlStringNullable(this SqlDataReader reader, int idx)
		{
			if (WriteHtmlNullFlag(reader, idx))
			{
				WriteHtmlString(reader, idx);
			}
		}

		public static void WriteHtmlGuid(this SqlDataReader reader, int idx)
		{
			Html.Cell(reader.GetGuid(idx).ToString());
		}

		public static void WriteHtmlGuidNullable(this SqlDataReader reader, int idx)
		{
			if (WriteHtmlNullFlag(reader, idx))
			{
				WriteHtmlGuid(reader, idx);
			}
		}
	}
}