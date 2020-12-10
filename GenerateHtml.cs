using System.Text;

namespace DataMover
{
	public enum Color
	{
		Nil,
		Green,
		Yellow,
		Red
	}

	public static class Html
	{
		private static StringBuilder _sb;

		public static void Start()
		{
			_sb = new StringBuilder(131072);
			Doc();
		}

		public static void Stop(string fileName)
		{
			EndDoc();

			var html = _sb.ToString();

			System.IO.File.WriteAllText(fileName, html);
		}

		public static void Doc()
		{
			_sb.AppendLine(
 @"<!DOCTYPE html>
<html>
<head>
<style>
section {
    margin-top: 30px;
    margin-bottom: 30px;
}
table, th, td {
    margin-top: 30px;
    margin-bottom: 30px;
    border: 1px solid black;
    border-collapse: collapse;
}
caption {
    text-align: left;
    font-weight: bold;
    font-style: italic;
    margin:4px;
}
th, td {
    padding: 5px;
    text-align: left;
}
</style>
</head>
<body>");
		}

		public static void EndDoc()
		{
			_sb.AppendLine(
 @"</body>
</html>");
		}

		public static void H1(string value, string argId = null)
		{
			if (argId == null)
				_sb.Append("<h1>");
			else
				_sb.AppendFormat("<h1 id=\"{0}\">", argId);

			_sb.Append(value);
			_sb.AppendLine("</h1>");
		}

		public static void Section()
		{
			_sb.Append("<section>");
		}

		public static void EndSection()
		{
			_sb.Append("</section>");
		}

		public static void Table()
		{
			_sb.Append("<table>");
		}

		public static void EndTable()
		{
			_sb.AppendLine("</table>");
		}

		public static void Caption(string value)
		{
			_sb.Append("<caption>");
			_sb.Append(value);
			_sb.AppendLine("</caption>");
		}

		public static void Row()
		{
			_sb.Append("<tr>");
		}

		public static void EndRow()
		{
			_sb.AppendLine("</tr>");
		}

		public static void CellHeading(string value)
		{
			_sb.Append("<th>");
			_sb.Append(value);
			_sb.AppendLine("</th>");
		}

		public static void Cell()
		{
			_sb.Append("<td>");
			_sb.AppendLine("</td>");
		}

		public static void Cell(string value, Color argColor = Color.Nil)
		{
			if (argColor == Color.Nil)
			{
				_sb.Append("<td>");
			}
			else
			{
				_sb.AppendFormat("<td bgcolor=\"{0}\">", argColor);
			}

			_sb.Append(value);
			_sb.AppendLine("</td>");
		}

		public static void Cell(bool value, Color argColor = Color.Nil)
		{
			if (argColor == Color.Nil)
			{
				_sb.Append("<td>");
			}
			else
			{
				_sb.AppendFormat("<td bgcolor=\"{0}\">", argColor);
			}

			_sb.Append(value);
			_sb.AppendLine("</td>");
		}

		public static void Cell(int value, Color argColor = Color.Nil)
		{
			if (argColor == Color.Nil)
			{
				_sb.Append("<td style=\"text-align:right\">");
			}
			else
				_sb.AppendFormat("<td bgcolor=\"{0}\" style=\"text-align:right\">", argColor);

			_sb.Append(value);
			_sb.AppendLine("</td>");
		}

		public static void Cell(int value, string argUnits)
		{
			_sb.Append("<td style=\"text-align:right\">");

			_sb.Append(value);
			_sb.Append(argUnits);
			_sb.AppendLine("</td>");
		}

		public static void Cell(long value)
		{
			_sb.Append("<td style=\"text-align:right\">");
			_sb.Append(value);
			_sb.AppendLine("</td>");
		}

		public static void Cell(short value)
		{
			_sb.Append("<td style=\"text-align:right\">");
			_sb.Append(value);
			_sb.AppendLine("</td>");
		}

		public static void Cell(byte value)
		{
			_sb.Append("<td style=\"text-align:right\">");
			_sb.Append(value);
			_sb.AppendLine("</td>");
		}

		public static void Cell(decimal value)
		{
			_sb.Append("<td style=\"text-align:right\">");
			_sb.Append(value);
			_sb.AppendLine("</td>");
		}

		public static void Cell(float value)
		{
			_sb.Append("<td style=\"text-align:right\">");
			_sb.Append(value.ToString("0.00"));
			_sb.AppendLine("</td>");
		}
	}
}
