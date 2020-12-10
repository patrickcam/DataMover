﻿using System;
using System.Data;
using System.Data.SqlTypes;
using System.IO;
using System.Xml;

namespace DataMover
{
	public static partial class ExtensionsDataRow
	{
		public static int? NullIfZero(this int val)
		{
			return val == 0 ? null : (int?)val;
		}

		public static void Get(this DataRow row, bool value, int idx)
		{
			row[idx] = value;
		}

		public static void GetNullIfFalse(this DataRow row, bool value, int idx)
		{
			row[idx] = value ? (object)true : DBNull.Value;
		}

		public static void Get(this DataRow row, bool? value, int idx)
		{
			row[idx] = value != null && value.Value ? (object)true : DBNull.Value;
		}

		public static void Get(this DataRow row, char value, int idx)
		{
			row[idx] = value;
		}

		public static void Get(this DataRow row, byte value, int idx)
		{
			row[idx] = value;
		}

		public static void Get(this DataRow row, byte? value, int idx)
		{
			row[idx] = value != null ? (object)value : DBNull.Value;
		}

		public static void Get(this DataRow row, short value, int idx)
		{
			row[idx] = value;
		}

		public static void Get(this DataRow row, short? value, int idx)
		{
			row[idx] = value != null ? (object)value : DBNull.Value;
		}

		public static void Get(this DataRow row, int value, int idx)
		{
			row[idx] = value;
		}

		public static void Get(this DataRow row, int? value, int idx)
		{
			row[idx] = value != null ? (object)value : DBNull.Value;
		}

		public static void GetNullIfNeg(this DataRow row, int? value, int idx)
		{
			row[idx] = value != null && value >= 0 ? (object)value : DBNull.Value;
		}

		public static void Get(this DataRow row, long value, int idx)
		{
			row[idx] = value;
		}

		public static void Get(this DataRow row, long? value, int idx)
		{
			row[idx] = value != null ? (object)value : DBNull.Value;
		}

		public static void Get(this DataRow row, float value, int idx)
		{
			row[idx] = value;
		}

		public static void Get(this DataRow row, float? value, int idx)
		{
			row[idx] = value != null ? (object)value : DBNull.Value;
		}

		public static void Get(this DataRow row, DateTime? value, int idx)
		{
			row[idx] = value != null ? (object)value : DBNull.Value;
		}

		public static void Get(this DataRow row, DateTime value, int idx)
		{
			row[idx] = (DateTime)SqlDateTime.MinValue <= value && value <= (DateTime)SqlDateTime.MaxValue
				? (object)value
				: DBNull.Value;
		}

		public static void Get(this DataRow row, string value, int idx)
		{
			row[idx] = value != null ? (object)value : DBNull.Value;
		}

		public static void Get(this DataRow row, Guid value, int idx)
		{
			row[idx] = value;
		}

		public static void Get(this DataRow row, Guid? value, int idx)
		{
			row[idx] = value != null ? (object)value : DBNull.Value;
		}

		public static void Get(this DataRow row, XmlDocument value, int idx)
		{
			row[idx] = value != null ? (object)value : DBNull.Value;
		}

		public static void GetNullIfEmpty(this DataRow row, string value, int idx)
		{
			row[idx] = string.IsNullOrEmpty(value) ? null : value;
		}

		public static void GetNullIfEmpty(this DataRow row, string value, int idx, int length)
		{
			if (string.IsNullOrEmpty(value))
				row[idx] = null;
			else
			{
				if (value.Length <= length)
					row[idx] = value;
				else
					row[idx] = value.Substring(0, length);
			}
		}

		public static void Get(this DataRow row, string value, int idx, int length)
		{
			if (idx != ExtensionsDataReader.UndefinedIndex && value != null)
			{
				if (value.Length <= length)
					row[idx] = value;
				else
					row[idx] = value.Substring(0, length);
			}
		}

		public static void Get(this DataRow row, decimal? value, int idx)
		{
			row[idx] = value != null ? (object)value : DBNull.Value;
		}

		public static void Set(this DataRow row, out byte value, int idx)
		{
			value = row[idx] != DBNull.Value ? (byte)row[idx] : (byte)0;
		}

		public static void Set(this DataRow row, out bool value, int idx)
		{
			value = row[idx] != DBNull.Value && (bool)row[idx];
		}

		public static void Set(this DataRow row, out bool? value, int idx)
		{
			value = row[idx] != DBNull.Value && (bool)row[idx];
		}

		public static void SetNullIf(this DataRow row, out bool? value, int idx)
		{
			value = row[idx] != DBNull.Value ? (bool?)(bool)row[idx] : null;
		}

		public static void Set(this DataRow row, out byte? value, int idx)
		{
			value = row[idx] != DBNull.Value ? (byte?)(byte)row[idx] : null;
		}

		public static void Set(this DataRow row, out short value, int idx)
		{
			value = row[idx] != DBNull.Value ? (short)row[idx] : (short)0;
		}

		public static void Set(this DataRow row, out short? value, int idx)
		{
			value = row[idx] != DBNull.Value ? (short?)(short)row[idx] : null;
		}

		public static void Set(this DataRow row, out int value, int idx)
		{
			value = row[idx] != DBNull.Value ? (int)row[idx] : 0;
		}

		public static void SetIf(this DataRow row, ref int value, int idx)
		{
			if (row[idx] == DBNull.Value) return;
			value = (int)row[idx];
		}

		public static void Set(this DataRow row, out int? value, int idx)
		{
			value = row[idx] != DBNull.Value ? (int?)(int)row[idx] : null;
		}

		public static void SetFromByte(this DataRow row, out int? value, int idx)
		{
			value = row[idx] != DBNull.Value ? (int?)(byte)row[idx] : null;
		}

		public static void Set(this DataRow row, out long value, int idx)
		{
			value = row[idx] != DBNull.Value ? (long)row[idx] : 0;
		}

		public static void SetIf(this DataRow row, ref long value, int idx)
		{
			if (row[idx] != DBNull.Value)
			{
				value = (long)row[idx];
			}
		}

		public static void Set(this DataRow row, out long? value, int idx)
		{
			value = row[idx] != DBNull.Value ? (long?)row[idx] : null;
		}

		public static void Set(this DataRow row, out DateTime value, int idx)
		{
			value = row[idx] != DBNull.Value ? (DateTime)row[idx] : DateTime.MinValue;
		}

		public static void SetMax(this DataRow row, out DateTime value, int idx)
		{
			value = row[idx] != DBNull.Value ? (DateTime)row[idx] : DateTime.MaxValue;
		}

		public static void Set(this DataRow row, out DateTime? value, int idx)
		{
			value = row[idx] != DBNull.Value ? (DateTime?)row[idx] : null;
		}

		public static void Set(this DataRow row, out Guid value, int idx)
		{
			value = row[idx] != DBNull.Value ? (Guid)row[idx] : Guid.Empty;
		}

		public static void Set(this DataRow row, out string value, int idx)
		{
			value = row[idx] != DBNull.Value ? (string)row[idx] : string.Empty;
		}

		public static void SetNullIf(this DataRow row, out string value, int idx)
		{
			value = row[idx] != DBNull.Value ? (string)row[idx] : null;
		}

		public static void Set(this DataRow row, out char value, int idx)
		{
			value = row[idx] != DBNull.Value ? (char)row[idx] : char.MinValue;
		}

		//!READBINARY

		//!VALUES
		public static DateTime? GetValueDateTimeNullable(this DataRow row, int idx, BinaryReader br)
		{
			throw new NotImplementedException();
		}

		public static byte GetValueByte(this DataRow row, int idx)
		{
			return row[idx] != DBNull.Value ? (Byte)row[idx] : (byte)0;
		}

		public static byte? GetValueByteNullable(this DataRow row, int idx)
		{
			return row[idx] != DBNull.Value ? (byte?)row[idx] : null;
		}

		public static bool GetValueBoolean(this DataRow row, int idx)
		{
			return row[idx] != DBNull.Value && (Boolean)row[idx];
		}

		public static bool? GetValueBooleanNullable(this DataRow row, int idx)
		{
			return row[idx] != DBNull.Value ? (bool?)row[idx] : null;
		}

		public static short GetValueInt16(this DataRow row, int idx)
		{
			return row[idx] != DBNull.Value ? (Int16)row[idx] : (short)0;
		}

		public static short? GetValueInt16Nullable(this DataRow row, int idx)
		{
			return row[idx] != DBNull.Value ? (short?)row[idx] : null;
		}

		public static int GetValueInt32(this DataRow row, int idx)
		{
			return row[idx] != DBNull.Value ? (Int32)row[idx] : 0;
		}

		public static int? GetValueInt32Nullable(this DataRow row, int idx)
		{
			return row[idx] != DBNull.Value ? (int?)row[idx] : null;
		}

		public static long GetValueInt64(this DataRow row, int idx)
		{
			return row[idx] != DBNull.Value ? (Int64)row[idx] : 0L;
		}

		public static long? GetValueInt64Nullable(this DataRow row, int idx)
		{
			return row[idx] != DBNull.Value ? (long?)row[idx] : null;
		}

		public static Type GetValueType(this DataRow row, int idx)
		{
			return row[idx] != DBNull.Value ? (Type)row[idx] : null;
		}

		public static DateTime GetValueDateTime(this DataRow row, int idx)
		{
			return row[idx] != DBNull.Value ? (DateTime)row[idx] : DateTime.MinValue;
		}

		public static DateTime? GetValueDateTimeNullable(this DataRow row, int idx)
		{
			return row[idx] != DBNull.Value ? (DateTime?)row[idx] : null;
		}

		public static decimal GetValueDecimal(this DataRow row, int idx)
		{
			return (Decimal)row[idx];
		}

		public static decimal? GetValueDecimalNullable(this DataRow row, int idx)
		{
			return row[idx] != DBNull.Value ? (decimal?)row[idx] : null;
		}

		public static string GetValueString(this DataRow row, int idx)
		{
			return row[idx] != DBNull.Value ? (string)row[idx] : string.Empty;
		}

		public static string GetValueStringNullable(this DataRow row, int idx)
		{
			return row[idx] != DBNull.Value ? (String)row[idx] : null;
		}

		public static Guid GetValueGuid(this DataRow row, int idx)
		{
			return row[idx] != DBNull.Value ? (Guid)row[idx] : Guid.Empty;
		}

		public static Guid? GetValueGuidNullable(this DataRow row, int idx)
		{
			return row[idx] != DBNull.Value ? (Guid?)(Guid)row[idx] : null;
		}
	}
}
