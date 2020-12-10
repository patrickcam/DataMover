using System;
using System.Data;
using System.IO;

namespace DataMover
{
	public static partial class ExtensionsDataRow
	{
		public static void ReadBinaryByte(this DataRow row, int idx, BinaryReader br)
		{
			row[idx] = br.ReadByte();
		}

		public static void ReadBinaryByteNullable(this DataRow row, int idx, BinaryReader br)
		{
			var isNotNull = br.ReadBoolean();
			if (isNotNull)
			{
				row[idx] = br.ReadByte();
			}
			else
			{
				row[idx] = DBNull.Value;
			}
		}

		public static void ReadBinaryBoolean(this DataRow row, int idx, BinaryReader br)
		{
			row[idx] = br.ReadBoolean();
		}

		public static void ReadBinaryBooleanNullable(this DataRow row, int idx, BinaryReader br)
		{
			var isNotNull = br.ReadBoolean();
			if (isNotNull)
			{
				row[idx] = br.ReadBoolean();
			}
			else
			{
				row[idx] = DBNull.Value;
			}
		}

		public static void ReadBinaryInt16(this DataRow row, int idx, BinaryReader br)
		{
			row[idx] = br.ReadInt16();
		}

		public static void ReadBinaryInt16Nullable(this DataRow row, int idx, BinaryReader br)
		{
			var isNotNull = br.ReadBoolean();
			if (isNotNull)
			{
				row[idx] = br.ReadInt16();
			}
			else
			{
				row[idx] = DBNull.Value;
			}
		}

		public static void ReadBinaryInt32(this DataRow row, int idx, BinaryReader br)
		{
			row[idx] = br.ReadInt32();
		}

		public static void ReadBinaryInt32Nullable(this DataRow row, int idx, BinaryReader br)
		{
			var isNotNull = br.ReadBoolean();
			if (isNotNull)
			{
				row[idx] = br.ReadInt32();
			}
			else
			{
				row[idx] = DBNull.Value;
			}
		}

		public static void ReadBinaryInt64(this DataRow row, int idx, BinaryReader br)
		{
			row[idx] = br.ReadInt64();
		}

		public static void ReadBinaryInt64Nullable(this DataRow row, int idx, BinaryReader br)
		{
			var isNotNull = br.ReadBoolean();
			if (isNotNull)
			{
				row[idx] = br.ReadInt64();
			}
			else
			{
				row[idx] = DBNull.Value;
			}
		}

		public static void ReadBinaryDateTime(this DataRow row, int idx, BinaryReader br)
		{
			var binary = br.ReadInt64();
			var tad = DateTime.FromBinary(binary);
			row[idx] = tad;
		}

		public static void ReadBinaryDateTimeNullable(this DataRow row, int idx, BinaryReader br)
		{
			var isNotNull = br.ReadBoolean();
			if (isNotNull)
			{
				ReadBinaryDateTime(row, idx, br);
			}
			else
			{
				row[idx] = DBNull.Value;
			}
		}

		public static DateTimeOffset ReadDateTimeOffset(this BinaryReader br)
		{
			var binDateTime = br.ReadInt64();
			var minutes = br.ReadInt16();

			var dateTime = DateTime.FromBinary(binDateTime);
			var dateTimeOffset = new DateTimeOffset(dateTime, TimeSpan.FromMinutes(minutes));

			return dateTimeOffset;
		}

		public static void ReadBinaryDateTimeOffset(this DataRow row, int idx, BinaryReader br)
		{
			row[idx] = br.ReadDateTimeOffset();
		}

		public static void ReadBinaryDateTimeOffsetNullable(this DataRow row, int idx, BinaryReader br)
		{
			var isNotNull = br.ReadBoolean();
			if (isNotNull)
			{
				row[idx] = br.ReadDateTimeOffset();
			}
			else
			{
				row[idx] = DBNull.Value;
			}
		}

		public static void ReadBinaryDecimal(this DataRow row, int idx, BinaryReader br)
		{
			row[idx] = br.ReadDecimal();
		}

		public static void ReadBinaryDecimalNullable(this DataRow row, int idx, BinaryReader br)
		{
			var isNotNull = br.ReadBoolean();
			if (isNotNull)
			{
				row[idx] = br.ReadDecimal();
			}
			else
			{
				row[idx] = DBNull.Value;
			}
		}

		public static void ReadBinaryString(this DataRow row, int idx, BinaryReader br)
		{
			row[idx] = br.ReadString();
		}

		public static void ReadBinaryStringNullable(this DataRow row, int idx, BinaryReader br)
		{
			var isNotNull = br.ReadBoolean();
			if (isNotNull)
			{
				row[idx] = br.ReadString();
			}
			else
			{
				row[idx] = DBNull.Value;
			}
		}

		public static Guid ReadGuid(this BinaryReader br)
		{
			var bytes = br.ReadBytes(16);
			return new Guid(bytes);
		}

		public static void ReadBinaryGuid(this DataRow row, int idx, BinaryReader br)
		{
			row[idx] = br.ReadGuid();
		}

		public static void ReadBinaryGuidNullable(this DataRow row, int idx, BinaryReader br)
		{
			var isNotNull = br.ReadBoolean();
			if (isNotNull)
			{
				row[idx] = br.ReadGuid();
			}
			else
			{
				row[idx] = DBNull.Value;
			}
		}

		public static void ReadBinaryBytes(this DataRow row, int idx, BinaryReader br)
		{
			var length = br.ReadInt32();
			row[idx] = br.ReadBytes(length);
		}

		public static void ReadBinaryBytesNullable(this DataRow row, int idx, BinaryReader br)
		{
			var isNotNull = br.ReadBoolean();
			if (isNotNull)
			{
				ReadBinaryBytes(row, idx, br);
			}
			else
			{
				row[idx] = DBNull.Value;
			}
		}

		public static void ReadBinaryXmlDocument(this DataRow row, int idx, BinaryReader br)
		{
			throw new NotImplementedException();
		}
	}
}