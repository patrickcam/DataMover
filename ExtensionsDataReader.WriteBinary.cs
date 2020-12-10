using System.Data.SqlClient;
using System.IO;

namespace DataMover
{
	public static partial class ExtensionsDataReader
	{
		private static bool WriteBinaryNullFlag(SqlDataReader reader, int idx, BinaryWriter bw)
		{
			var isNotNull = !reader.IsDBNull(idx);
			bw.Write(isNotNull);
			return isNotNull;
		}

		public static void WriteBinaryBytes(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			var bytes = (byte[])reader[idx];
			var length = bytes.Length;

			bw.Write(length);
			bw.Write(bytes);
		}

		public static void WriteBinaryBytesNullable(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			if (WriteBinaryNullFlag(reader, idx, bw))
			{
				WriteBinaryBytes(reader, idx, bw);
			}
		}

		public static void WriteBinaryByte(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			bw.Write(reader.GetByte(idx));
		}

		public static void WriteBinaryByteNullable(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			if (WriteBinaryNullFlag(reader, idx, bw))
			{
				WriteBinaryByte(reader, idx, bw);
			}
		}

		public static void WriteBinaryBoolean(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			bw.Write(reader.GetBoolean(idx));
		}

		public static void WriteBinaryBooleanNullable(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			if (WriteBinaryNullFlag(reader, idx, bw))
			{
				WriteBinaryBoolean(reader, idx, bw);
			}
		}

		public static void WriteBinaryInt16(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			bw.Write(reader.GetInt16(idx));
		}

		public static void WriteBinaryInt16Nullable(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			if (WriteBinaryNullFlag(reader, idx, bw))
			{
				WriteBinaryInt16(reader, idx, bw);
			}
		}

		public static void WriteBinaryInt32(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			bw.Write(reader.GetInt32(idx));
		}

		public static void WriteBinaryInt32Nullable(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			if (WriteBinaryNullFlag(reader, idx, bw))
			{
				WriteBinaryInt32(reader, idx, bw);
			}
		}

		public static void WriteBinaryInt64(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			bw.Write(reader.GetInt64(idx));
		}

		public static void WriteBinaryInt64Nullable(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			if (WriteBinaryNullFlag(reader, idx, bw))
			{
				WriteBinaryInt64(reader, idx, bw);
			}
		}

		public static void WriteBinaryDateTime(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			var binary = reader.GetDateTime(idx).ToBinary();
			bw.Write(binary);
		}

		public static void WriteBinaryDateTimeNullable(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			if (WriteBinaryNullFlag(reader, idx, bw))
			{
				WriteBinaryDateTime(reader, idx, bw);
			}
		}

		public static void WriteBinaryDateTimeOffset(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			var dateTimeOffset = reader.GetDateTimeOffset(idx);
			var dateTime = dateTimeOffset.DateTime;
			var minutes = (short)dateTimeOffset.Offset.TotalMinutes;

			bw.Write(dateTime.ToBinary());
			bw.Write(minutes);
		}

		public static void WriteBinaryDateTimeOffsetNullable(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			if (WriteBinaryNullFlag(reader, idx, bw))
			{
				WriteBinaryDateTimeOffset(reader, idx, bw);
			}
		}

		public static void WriteBinaryDecimal(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			bw.Write(reader.GetDecimal(idx));
		}

		public static void WriteBinaryDecimalNullable(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			if (WriteBinaryNullFlag(reader, idx, bw))
			{
				WriteBinaryDecimal(reader, idx, bw);
			}
		}

		public static void WriteBinaryString(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			bw.Write(reader.GetString(idx));
		}

		public static void WriteBinaryStringNullable(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			if (WriteBinaryNullFlag(reader, idx, bw))
			{
				WriteBinaryString(reader, idx, bw);
			}
		}

		public static void WriteBinaryGuid(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			bw.Write(reader.GetGuid(idx).ToByteArray());
		}

		public static void WriteBinaryGuidNullable(this SqlDataReader reader, int idx, BinaryWriter bw)
		{
			if (WriteBinaryNullFlag(reader, idx, bw))
			{
				WriteBinaryGuid(reader, idx, bw);
			}
		}
	}
}